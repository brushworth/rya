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
package org.apache.rya.accumulo.query;

import org.apache.accumulo.core.util.NamingThreadFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This pool progressively allocates more threads to the pool for each additional query, so no additional query
 * is starved of being able to request query threads. It gives out the most threads for the first queries,
 * but all subsequent queries will get at least a small number of threads, so that the overall query can
 * continue to keep executing, through a Join operation for example.
 */
public class AccumuloIteratorThreadPool {

    private static final Log logger = LogFactory.getLog(AccumuloIteratorThreadPool.class);
    private static final Map<String, ThreadPoolExecutor> threadPoolPerServer = new ConcurrentSkipListMap<>();
    private static final AtomicInteger LEVELS_DEEP_IN_POOL = new AtomicInteger(0);
    private static final AtomicInteger currentNumThreadsPerServer = new AtomicInteger(0);

    private final int totalNumThreadsPerServer;

    static {
        new Thread(() -> {
            try {
                while (logger.isInfoEnabled()) {
                    Thread.sleep(15000);
                    //logger.info("iteratorProcessingQueue=" + iteratorProcessingQueue.size() + " accumuloDataQueue=" + accumuloDataQueue.size());
                    //if (iteratorProcessingQueue.size() == 1) {
                    //    logger.info("iteratorProcessingQueue=" + iteratorProcessingQueue.toString());
                    //}
                    logger.info("currentNumThreadsPerServer=" + currentNumThreadsPerServer.get());
                    for (Map.Entry<String, ThreadPoolExecutor> executor : threadPoolPerServer.entrySet()) {
                        if (executor.getValue().getActiveCount() > 0) {
                            logger.info("executor=" + executor);
                        }
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }, "AccumuloIteratorThreadPool").start();
    }

    public AccumuloIteratorThreadPool(int totalNumThreadsPerServer) {
        LEVELS_DEEP_IN_POOL.incrementAndGet();
        this.totalNumThreadsPerServer = totalNumThreadsPerServer;
    }

    public void close() {
        LEVELS_DEEP_IN_POOL.decrementAndGet();
    }

    protected static void reset() {
        LEVELS_DEEP_IN_POOL.set(0);
        threadPoolPerServer.clear();
    }

    public synchronized int getCurrentNumThreadsPerServer() {
        // How many simultaneous instances of this class are currently running?
        int levels = LEVELS_DEEP_IN_POOL.get();

        // This will give out a lot of threads early, but less and less as more as required.
        // It limits at totalNumThreadsPerServer, making currentNumThreadsPerServer higher each time.
        currentNumThreadsPerServer.set(Math.round(totalNumThreadsPerServer * (levels / (1.0f + levels))));

        return currentNumThreadsPerServer.get();
    }

    public synchronized void execute(String tabletServerName, Runnable fetcher) {
        // Create the thread pool and set the maximum amount of threads per Accumulo server, if doesn't already exist
        int numThreads = getCurrentNumThreadsPerServer();
        ThreadPoolExecutor threadPoolExecutor = threadPoolPerServer.getOrDefault(
                tabletServerName,
                new ThreadPoolExecutor(numThreads, numThreads,
                        30L, TimeUnit.SECONDS, new LiFoBlockingQueue(),
                        new NamingThreadFactory("Iterator " + tabletServerName + " thread")));
        threadPoolPerServer.put(tabletServerName, threadPoolExecutor);

        // Queue the thread for execution
        threadPoolExecutor.execute(fetcher);

        // Increase or decrease the core size as required, taking the most recently added runnable
        threadPoolExecutor.setCorePoolSize(numThreads);
        threadPoolExecutor.setMaximumPoolSize(numThreads);
        //logger.info("tabletServerName=" + tabletServerName + " threadPoolExecutor=" + threadPoolExecutor);
    }

    protected List<Runnable> getTaskOrder(String tabletServerName) {
        return new ArrayList<>(threadPoolPerServer.get(tabletServerName).getQueue());
    }

    protected boolean isIdle() {
        for (Map.Entry<String, ThreadPoolExecutor> entry : threadPoolPerServer.entrySet()) {
            String tabletServerName = entry.getKey();
            ThreadPoolExecutor threadPoolExecutor = entry.getValue();
            //logger.info("tabletServerName=" + tabletServerName + " threadPoolExecutor=" + threadPoolExecutor);
            if (threadPoolExecutor.getActiveCount() > 0 || threadPoolExecutor.getQueue().size() > 0) {
                return false;
            }
        }
        return true;
    }

}
