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

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.apache.rya.api.RdfCloudTripleStoreUtils;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.resolver.RyaTripleContext;
import org.apache.rya.api.resolver.triple.TripleRow;
import org.apache.rya.api.resolver.triple.TripleRowResolverException;
import org.eclipse.rdf4j.query.BindingSet;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Date: 7/17/12
 * Time: 11:48 AM
 */
public class RyaStatementBindingSetKeyValueIterator implements RyaKeyValueIterator {
    private static final Log logger = LogFactory.getLog(RyaStatementBindingSetKeyValueIterator.class);
    private Iterator<Map.Entry<Key, Value>> dataIterator;
    private TABLE_LAYOUT tableLayout;
    private Long maxResults = -1L;
    private ScannerBase scanner;
    private RangeBindingSetEntries rangeMap;
    private Iterator<BindingSet> bsIter;
    private RyaStatement statement;
	private RyaTripleContext ryaContext;

    public RyaStatementBindingSetKeyValueIterator(TABLE_LAYOUT tableLayout, RyaTripleContext context, ScannerBase scanner, RangeBindingSetEntries rangeMap) {
        this(tableLayout, context, scanner.iterator(), rangeMap);
        this.scanner = scanner;
    }

    public RyaStatementBindingSetKeyValueIterator(TABLE_LAYOUT tableLayout, RyaTripleContext ryaContext, Iterator<Map.Entry<Key, Value>> dataIterator, RangeBindingSetEntries rangeMap) {
        this.tableLayout = tableLayout;
        this.ryaContext = ryaContext;
        this.dataIterator = dataIterator;
        this.rangeMap = rangeMap;
    }

    @Override
    public void close() throws RyaDAOException {
        dataIterator = null;
        if (scanner != null) {
            scanner.close();
        }
    }

    @Override
    public boolean isClosed() throws RyaDAOException {
        return dataIterator == null;
    }

    @Override
    public boolean hasNext() throws RyaDAOException {
        if (isClosed()) {
            return false;
        }
        if (maxResults != 0) {
            if (bsIter != null && bsIter.hasNext()) {
                return true;
            }
            if (dataIterator.hasNext()) {
                return true;
            } else {
                maxResults = 0l;
                return false;
            }
        }
        return false;
    }

    @Override
    public Map.Entry<RyaStatement, BindingSet> next() throws RyaDAOException {
        if (!hasNext() || isClosed()) {
            throw new NoSuchElementException();
        }

        try {
            while (true) {
                if (bsIter != null && bsIter.hasNext()) {
                    maxResults--;
                    return new RdfCloudTripleStoreUtils.CustomEntry<>(statement, bsIter.next());
                }

                if (dataIterator.hasNext()) {
                    Map.Entry<Key, Value> next = dataIterator.next();
                    Key key = next.getKey();
                    statement = ryaContext.deserializeTriple(tableLayout,
                            new TripleRow(key.getRowData().toArray(), key.getColumnFamilyData().toArray(), key.getColumnQualifierData().toArray(),
                                    key.getTimestamp(), key.getColumnVisibilityData().toArray(), next.getValue().get()));
                    if (next.getValue() != null) {
                        statement.setValue(next.getValue().get());
                    }
                    Collection<BindingSet> bindingSets = rangeMap.containsKey(key);
                    if (!bindingSets.isEmpty()) {
                        bsIter = bindingSets.iterator();
                    } else {
                        bsIter = null;
                    }
                } else {
                    break;
                }
            }
            return null;
        } catch (TripleRowResolverException e) {
            throw new RyaDAOException(e);
        }
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
}
