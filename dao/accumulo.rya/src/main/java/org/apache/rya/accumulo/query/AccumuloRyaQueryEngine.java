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

import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.iterators.user.TimestampFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.persist.query.RyaQueryEngine;
import org.apache.rya.api.query.strategy.ByteRange;
import org.apache.rya.api.query.strategy.TriplePatternStrategy;
import org.apache.rya.api.resolver.RyaTripleContext;
import org.apache.rya.api.resolver.triple.TripleRowRegex;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.BindingSet;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;
import java.util.regex.Pattern;

import static org.apache.rya.api.RdfCloudTripleStoreUtils.layoutToTable;

/**
 * Date: 7/17/12 Time: 9:28 AM
 */
public class AccumuloRyaQueryEngine implements RyaQueryEngine<AccumuloRdfConfiguration> {
    private static final Log logger = LogFactory.getLog(AccumuloRyaQueryEngine.class);

    private AccumuloRdfConfiguration configuration;
    private Connector connector;
    private RyaTripleContext ryaContext;
    private final Map<TABLE_LAYOUT, KeyValueToRyaStatementFunction> keyValueToRyaStatementFunctionMap = new HashMap<>();
    
    public AccumuloRyaQueryEngine(Connector connector, AccumuloRdfConfiguration conf) {
        this.connector = connector;
        this.configuration = conf;
        ryaContext = RyaTripleContext.getInstance(conf);
        keyValueToRyaStatementFunctionMap.put(TABLE_LAYOUT.SPO, new KeyValueToRyaStatementFunction(TABLE_LAYOUT.SPO, ryaContext));
        keyValueToRyaStatementFunctionMap.put(TABLE_LAYOUT.PO, new KeyValueToRyaStatementFunction(TABLE_LAYOUT.PO, ryaContext));
        keyValueToRyaStatementFunctionMap.put(TABLE_LAYOUT.OSP, new KeyValueToRyaStatementFunction(TABLE_LAYOUT.OSP, ryaContext));
    }

    @Override
    public CloseableIteration<Map.Entry<RyaStatement, BindingSet>, RyaDAOException> queryWithBindingSet(
            SetMultimap<RyaStatement, BindingSet> stmts, AccumuloRdfConfiguration conf) throws RyaDAOException {
        if (conf == null) {
            conf = configuration;
        }

        // Query configuration
        Authorizations authorizations = conf.getAuthorizations();
        Long currentTime = conf.getStartTime();
        Long ttl = conf.getTtl();
        Long maxResults = conf.getLimit();
        Integer numThreads = conf.getNumThreads();

        if (logger.isDebugEnabled()) {
            try {
                throw new RuntimeException("Showing query stacktrace...");
            } catch (RuntimeException e) {
                logger.debug("Showing query stacktrace...", e);
            }
        }
        logger.info("Scanning with up to " + String.format("%5d", numThreads) + " threads " +
                "for " + String.format("%6d", stmts.size()) + " statements " +
                "[thread " + Thread.currentThread().getId() + "]"
        );

        try {
            RangeBindingSetEntries rangeMap = new RangeBindingSetEntries();

            TriplePatternStrategy strategy = null;

            // We need to keep track of the column families and qualifiers that have been requested.
            // Don't need to worry about duplicate ranges because they take into account the column binding in the hashCode.
            SortedSetMultimap<Pair<String, String>, Range> columnFamilyQualifierRange = TreeMultimap.create(Ordering.usingToString(), Ordering.natural());

            for (Map.Entry<RyaStatement, BindingSet> stmtbs : stmts.entries()) {
                RyaStatement stmt = stmtbs.getKey();
                BindingSet bs = stmtbs.getValue();
                strategy = ryaContext.retrieveStrategy(stmt);
                if (strategy == null) {
                    throw new IllegalArgumentException("TriplePattern[" + stmt + "] not supported");
                }

                // Create a range object to set scanner range method later.
                Range range;
                byte[] exactKey = strategy.defineExact(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(), stmt.getContext(), conf);
                if (exactKey != null) {
                    // Utilise Accumulo bloom filters by asking for a specific row
                    range = Range.exact(new Text(exactKey));
                } else {
                    // Create a normal range
                    ByteRange byteRange = strategy.defineRange(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(), stmt.getContext(), conf);
                    range = new Range(new Text(byteRange.getStart()), new Text(byteRange.getEnd()));
                }

                // Include in the range object the column families (if any) that we will be looking
                // for. This doesn't perform column filtering, but if the underlying table
                // is configured for locality groups, this will enable irrelevant tablets to be skipped,
                // which will improve scan performance.
                // Also store the column family and qualifier to perform the filtering later in the method.
                byte[] byteRangeColumn = strategy.defineColumn(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(), stmt.getContext(), conf);
                if (byteRangeColumn != null && byteRangeColumn.length > 0) {
                    range = range.bound(
                            new Column(byteRangeColumn, stmt.getQualifer() != null ? stmt.getQualifer().getBytes(StandardCharsets.UTF_8): null, stmt.getColumnVisibility()),
                            new Column(byteRangeColumn, stmt.getQualifer() != null ? stmt.getQualifer().getBytes(StandardCharsets.UTF_8): null, stmt.getColumnVisibility())
                    );
                    // Make sure this range is stored AFTER bounding it (which changes it)
                    columnFamilyQualifierRange.put( new Pair<>(new String(byteRangeColumn, StandardCharsets.UTF_8), stmt.getQualifer()), range);
                } else if (stmt.getQualifer() != null && !stmt.getQualifer().isEmpty()) {
                    columnFamilyQualifierRange.put(new Pair<>(null, stmt.getQualifer()), range);
                } else {
                    columnFamilyQualifierRange.put(new Pair<>(null, null), range);
                }

                // ranges gets a Range that has no Column bounds, but
                // rangeMap gets a Range that does have Column bounds
                // If we inserted multiple Ranges with the same Row (but
                // distinct Column bounds) into the Set ranges, we would get
                // duplicate
                // results when the Row is not exact. So RyaStatements that
                // differ only in their context are all mapped to the same
                // Range (with no Column bounds) for scanning purposes.
                // However, context information is included in a Column that
                // bounds the Range inserted into rangeMap. This is because
                // in the class {@link RyaStatementBindingSetKeyValueIterator},
                // the rangeMap is
                // used to join the scan results with the BindingSets to produce
                // the query results. The additional ColumnFamily info is
                // required in this join
                // process to allow for the Statement contexts to be compared
                // with the BindingSet contexts
                // See {@link RangeBindingSetEntries#containsKey}.
                //ranges.add(range);
                rangeMap.put(range, bs);
            }

            TABLE_LAYOUT layout = strategy.getLayout();
            String table = layoutToTable(layout, conf);

            String regexSubject = conf.getRegexSubject();
            String regexPredicate = conf.getRegexPredicate();
            String regexObject = conf.getRegexObject();
            TripleRowRegex tripleRowRegex = strategy.buildRegex(regexSubject, regexPredicate, regexObject, null, null);


            RyaKeyValueIterator iterator;
            if (columnFamilyQualifierRange.keySet().size() <= 1) {
                // Only use a single BatchScanner if you are looking for a maximum of one column combination because
                // BatchScanners cannot look for different columns in a set of ranges.
                BatchScanner scanner = connector.createBatchScanner(table, authorizations, numThreads);
                scanner.setRanges(new HashSet<>(columnFamilyQualifierRange.values())); // Deduplicate ranges
                Pair<String, String> columns = columnFamilyQualifierRange.keySet().iterator().next();
                fillScanner(scanner, columns, ttl, currentTime, tripleRowRegex, conf);
                if (rangeMap.hasBindingSet()) {
                    iterator = new RyaStatementBindingSetKeyValueIterator(layout, ryaContext, scanner, rangeMap);
                } else {
                    iterator = new RyaStatementKeyValueIterator(layout, ryaContext, scanner);
                }
            } else {
                Iterator<Map.Entry<Key, Value>>[] iters = new Iterator[columnFamilyQualifierRange.size()];
                int i = 0;
                for (Pair<String, String> columns : columnFamilyQualifierRange.keySet()) {
                    SortedSet<Range> ranges = columnFamilyQualifierRange.get(columns);
                    for (Range range : ranges) {
                        Scanner scanner = connector.createScanner(table, authorizations);
                        scanner.setRange(range);
                        fillScanner(scanner, columns, ttl, currentTime, tripleRowRegex, conf);
                        iters[i] = scanner.iterator();
                        i++;
                    }
                }
                if (rangeMap.hasBindingSet()) {
                    iterator = new RyaStatementBindingSetKeyValueIterator(layout, ryaContext, Iterators.concat(iters), rangeMap);
                } else {
                    iterator = new RyaStatementKeyValueIterator(layout, ryaContext, Iterators.concat(iters));
                }
            }

            if (maxResults != null) {
                iterator.setMaxResults(maxResults);
            }

            return iterator;
        } catch (Exception e) {
            throw new RyaDAOException(e);
        }

    }

    protected void fillScanner(ScannerBase scanner, Pair<String, String> column, Long ttl, Long currentTime,
                               TripleRowRegex tripleRowRegex, RdfCloudTripleStoreConfiguration conf) {

        // We can only use this method when we are looking for a single column.
        // If we are looking for multiple ranges in a batch, we need to be careful that each range
        // is looking for the same column.
        if (column != null) {
            String family = column.getFirst();
            String qualifier = column.getSecond();
            if (family != null && qualifier != null) {
                scanner.fetchColumn(new Text(family), new Text(qualifier));
            } else if (family != null) {
                scanner.fetchColumnFamily(new Text(family));
            } else if (qualifier != null) {
                IteratorSetting setting = new IteratorSetting(8, "riq", RegExFilter.class.getName());
                // Ensure qualifier string is regex friendly by quoting the whole string using \Q and \E
                RegExFilter.setRegexs(setting, null, null, Pattern.quote(qualifier), null, false);
                scanner.addScanIterator(setting);
            }
        }

        if (ttl != null) {
            IteratorSetting setting = new IteratorSetting(9, "fi", TimestampFilter.class.getName());
            if (currentTime != null) {
                TimestampFilter.setStart(setting, currentTime - ttl, true);
                TimestampFilter.setEnd(setting, currentTime, true);
            } else {
                TimestampFilter.setStart(setting, System.currentTimeMillis() - ttl, true);
                TimestampFilter.setEnd(setting, System.currentTimeMillis(), true);
            }
            scanner.addScanIterator(setting);
        }

        if (tripleRowRegex != null) {
            IteratorSetting setting = new IteratorSetting(11, "ri", RegExFilter.class.getName());
            RegExFilter.setRegexs(setting, tripleRowRegex.getRow(), tripleRowRegex.getColumnFamily(), tripleRowRegex.getColumnQualifier(), null, false);
            scanner.addScanIterator(setting);
        }

        if (conf instanceof AccumuloRdfConfiguration) {
            // TODO should we take the iterator settings as is or should we
            // adjust the priority based on the above?
            for (IteratorSetting itr : ((AccumuloRdfConfiguration) conf).getAdditionalIterators()) {
                scanner.addScanIterator(itr);
            }
        }
    }

    @Override
    public void setConf(AccumuloRdfConfiguration conf) {
        this.configuration = conf;
    }

    @Override
    public AccumuloRdfConfiguration getConf() {
        return configuration;
    }

    @Override
    public void close() throws IOException {
    }
}
