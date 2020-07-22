package org.apache.rya.accumulo.query;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.admin.Locations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.NamingThreadFactory;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.apache.rya.api.RdfCloudTripleStoreUtils;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.resolver.RyaTripleContext;
import org.apache.rya.api.resolver.triple.TripleRow;
import org.apache.rya.api.resolver.triple.TripleRowResolverException;
import org.eclipse.rdf4j.query.BindingSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Date: 7/17/12
 * Time: 11:48 AM
 */
@Beta
public class RyaStatementBindingSetThreadPoolKeyValueIterator implements RyaKeyValueIterator {

    private static final Log logger = LogFactory.getLog(RyaStatementBindingSetThreadPoolKeyValueIterator.class);

    private static final Map<String, ThreadPoolExecutor> threadPoolPerServer = new HashMap<>();

    private final TABLE_LAYOUT tableLayout;
    private final RyaTripleContext ryaContext;
    private final Map<ScannerBase, BindingSet> accumuloScanners;
    private final HashSet<AccumuloFetcherRunnable> iteratorProcessingQueue;
    //private int iteratorProcessingQueueCount;
    private final LinkedBlockingQueue<Pair<Map.Entry<Key, Value>, BindingSet>> accumuloDataQueue;

    private boolean closed = false;
    private int count = 0;
    private Long maxResults = -1L;
    private RyaStatement statement;
    private Iterator<BindingSet> bsIter;
    private int waitTime = 1;

    public RyaStatementBindingSetThreadPoolKeyValueIterator(AccumuloRdfConfiguration conf, TABLE_LAYOUT tableLayout, RyaTripleContext ryaContext, Map<ScannerBase, BindingSet> accumuloScanners, Map<ScannerBase, Range> accumuloRanges, Locations locations, int numberServers) {
        Preconditions.checkNotNull(conf);
        Preconditions.checkNotNull(tableLayout);
        Preconditions.checkNotNull(ryaContext);
        Preconditions.checkNotNull(accumuloScanners);
        Preconditions.checkArgument(conf.getNumThreads() > 0);
        Preconditions.checkArgument(accumuloScanners.size() > 0);
        Preconditions.checkArgument(accumuloRanges.size() > 0);
        Preconditions.checkArgument(numberServers > 0);

        this.tableLayout = tableLayout;
        this.ryaContext = ryaContext;
        this.accumuloScanners = accumuloScanners;

        // Create a fixed-size bounded queue with a single thread for each Accumulo scanner
        this.iteratorProcessingQueue = new HashSet<>();
        //this.iteratorProcessingQueueCount = 0;

        // Don't allow unlimited memory, only take enough for a decent buffer
        Integer batchSize = conf.getBatchSize();
        if (batchSize == null) batchSize = Constants.SCAN_BATCH_SIZE * numberServers;
        this.accumuloDataQueue = new LinkedBlockingQueue<>();
        logger.info("accumuloDataQueue.batchSize="+batchSize+" (now unlimited)");

        // Set the maximum number of results asked for in the query, if any
        if (conf.getLimit() != null) {
            this.maxResults = conf.getLimit();
        }

        // Use a thread for every disk in a typical Accumulo server, but no more than the Tomcat server can cope with
        int numThreads = 15; //Math.min(conf.getNumThreads() / numberServers, conf.getNumDisksPerServer());

        logger.info("Starting " + String.format("%6d", accumuloScanners.size()) + " scanners across " +
                String.format("%4d", numberServers) + " servers using max " +
                String.format("%5d", numThreads) + " threads per server [thread " + Thread.currentThread().getId() + "]");

        // Start up a thread pool of processes that will read data from Accumulo
        // and put it into a single data queue for the main thread to read from.
        // Maintain a separate thread pool for each Accumulo server, so not to overwhelm any particular server.
        // Synchronize this because more than one SPAQRL query can be executed in parallel, so avoid race conditions.
        synchronized (threadPoolPerServer) {
            // Get the keySet() here so we don't call the same scanner more than once.
            for (Map.Entry<ScannerBase, BindingSet> entry : this.accumuloScanners.entrySet()) {
                ScannerBase scanner = entry.getKey();
                BindingSet bs = entry.getValue();

                String tabletServerName = "default";
                // If not a mock Accumulo instance
                if (locations != null) {
                    Range range = accumuloRanges.get(scanner);
                    // Locate where in the cluster the data will be
                    List<TabletId> tablets = locations.groupByRange().get(range);
                    if (tablets != null && !tablets.isEmpty()) {
                        TabletId tabletId = tablets.get(0);
                        tabletServerName = locations.getTabletLocation(tabletId);
                    }
                }

                // Create the thread pool and set the maximum amount of threads per Accumulo server, if doesn't already exist
                ThreadPoolExecutor threadPoolExecutor = threadPoolPerServer.getOrDefault(
                        tabletServerName,
                        new ThreadPoolExecutor(numThreads, numThreads,
                                120L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
                                new NamingThreadFactory("Iterator "+tabletServerName+" thread")));
                threadPoolPerServer.put(tabletServerName, threadPoolExecutor);
                logger.info("tabletServerName="+tabletServerName+" threadPoolExecutor="+threadPoolExecutor);

                // Prepare work in thread pools
                AccumuloFetcherRunnable fetcher = new AccumuloFetcherRunnable(scanner, bs);
                iteratorProcessingQueue.add(fetcher);

                // Execute the thread
                threadPoolExecutor.execute(fetcher);
            }
        }
    }

    @Override
    public void close() throws RyaDAOException {
        closed = true;

        // Tell rogue AccumuloFetcherRunnable threads to stop (if any)
        for (AccumuloFetcherRunnable runnable : iteratorProcessingQueue) {
            runnable.interrupt();
        }

        // Close Accumulo scanners
        for (ScannerBase scanner : accumuloScanners.keySet()) {
            scanner.close();
        }
    }

    public boolean isClosed() throws RyaDAOException {
        return closed;
    }

    @Override
    public boolean hasNext() throws RyaDAOException {
        try {
            do {
                // If the iterator is closed
                if (isClosed()) {
                    logger.info("Returning " + count + " statements (closed)  [thread " + Thread.currentThread().getId() + "]");
                    return false;
                }

                // Once we reach the maximum number of results asked for in the query
                if (maxResults == 0) { // Negative numbers are acceptable for when no max limit applies
                    logger.info("Returning " + count + " statements (max reached) [thread " + Thread.currentThread().getId() + "]");
                    close();
                    return false;
                }

                // When hasNext() is called twice in a row, the data is already waiting.
                // Note: If one of these is true, they should all be true.
                if (statement != null && bsIter != null && bsIter.hasNext()) {
                    return true;
                }

                // Wait a small period of time for new data to become available (data can stop at any time)
                //logger.info("accumuloDataQueue.poll()");
                Pair<Map.Entry<Key, Value>, BindingSet> next = accumuloDataQueue.poll(waitTime, TimeUnit.MILLISECONDS);
                // poll() will return null if data is not yet available
                if (next != null) {
                    //logger.info("accumuloDataQueue.poll()=" + next);
                    count++;
                    Key key = next.getFirst().getKey();
                    statement = ryaContext.deserializeTriple(tableLayout, new TripleRow(
                            key.getRowData().toArray(),
                            key.getColumnFamilyData().toArray(),
                            key.getColumnQualifierData().toArray(),
                            key.getTimestamp(),
                            key.getColumnVisibilityData().toArray(),
                            next.getFirst().getValue().get()));

                    bsIter = Iterators.singletonIterator(next.getSecond());
                    return true;
                } else {
                    //waitTime *= 2;
                }

                // If there is no new data waiting and all the iterator threads are finished, the query is finished!
                //logger.info("iteratorProcessingQueue=" + iteratorProcessingQueue.size() + " accumuloDataQueue="+accumuloDataQueue.size());
                //if (iteratorProcessingQueue.size() == 1) {
                //    logger.info("iteratorProcessingQueue="+iteratorProcessingQueue.toString());
                //}
                if (iteratorProcessingQueue.size() < 1 && accumuloDataQueue.isEmpty()) {
                    // We're all done here
                    maxResults = 0L;
                    logger.info("Returning " + count + " statements (done) [thread " + Thread.currentThread().getId() + "]");
                    return false;
                }

            } while (true); // Waiting for data to be returned by Accumulo
        } catch (TripleRowResolverException e) {
            throw new RyaDAOException(e);
        } catch (InterruptedException e) {
            // Do nothing
            return false;
        }
    }

    @Override
    public Map.Entry<RyaStatement, BindingSet> next() throws RyaDAOException {
        if (logger.isTraceEnabled()) {
            logger.trace("Returning: " +
                    (statement.getSubject() != null ? statement.getSubject().toString() : null) + " " +
                    (statement.getPredicate() != null ? statement.getPredicate().toString() : null) + " " +
                    (statement.getObject() != null ? statement.getObject().toString() : null) + " " +
                    (statement.getContext() != null ? statement.getContext().toString() : null) + " " +
                    "[" + Thread.currentThread().getName() + "]"
            );
        }

        if (hasNext()) {
            maxResults--; // This will go negative in the absence of a max limit
            return new RdfCloudTripleStoreUtils.CustomEntry<>(statement, bsIter.next());
        }

        throw new NoSuchElementException();
    }

    @Override
    public void remove() throws RyaDAOException {
        next();
    }

    @Override
    public Long getMaxResults() {
        return maxResults;
    }

    @Override
    public void setMaxResults(Long maxResults) {
        this.maxResults = maxResults;
    }

    /**
     * Asynchronously retrieve records from Accumulo and add them to a single queue for the main thread to read.
     */
    private class AccumuloFetcherRunnable implements Runnable {

        private final ScannerBase scanner;
        private final BindingSet bs;

        private boolean interrupted;
        private Thread thread;

        public AccumuloFetcherRunnable(final ScannerBase scanner, BindingSet bs) {
            this.scanner = scanner;
            this.bs = bs;
            this.interrupted = false;
            //iteratorProcessingQueueCount++;
        }

        @Override
        public void run() {
            try {
                thread = Thread.currentThread();
                // Don't start scanner if we don't need the results anymore
                if (!interrupted && !thread.isInterrupted()) {
                    // Start the iterator within the thread pool, to control the cluster load
                    Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
                    //logger.info("scanner.iterator()");

                    while (!interrupted && !thread.isInterrupted() && !isClosed() && iterator.hasNext()) {
                        Map.Entry<Key, Value> keyValue = iterator.next();
                        // put() will wait if necessary for the buffer to have space
                        //logger.info("iterator.next() having iteratorProcessingQueueCount=" + iteratorProcessingQueue.size());
                        accumuloDataQueue.put(new Pair<>(keyValue, bs));
                        //logger.info("accumuloDataQueue.put()");
                    }
                }
            } catch(InterruptedException e){
                logger.warn("InterruptedException on accumuloDataQueue", e);
            } finally {
                scanner.close();
                Preconditions.checkState(iteratorProcessingQueue.remove(this));
                //iteratorProcessingQueueCount--;
                logger.info("iteratorProcessingQueue.remove()");
            }
        }

        public void interrupt() {
            if (thread != null) {
                thread.interrupt();
            }
            interrupted = true;
            //scanner.close();
            //Preconditions.checkState(iteratorProcessingQueue.remove(this));
        }

        @Override
        public int hashCode() {
            return scanner.hashCode();
        }
    }
}
