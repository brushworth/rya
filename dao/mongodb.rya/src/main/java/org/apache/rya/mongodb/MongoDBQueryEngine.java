/**
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
package org.apache.rya.mongodb;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.persist.query.RyaQueryEngine;
import org.apache.rya.mongodb.dao.MongoDBStorageStrategy;
import org.apache.rya.mongodb.dao.SimpleMongoDBStorageStrategy;
import org.apache.rya.mongodb.iter.RyaStatementBindingSetCursorIterator;
import org.bson.Document;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.BindingSet;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Date: 7/17/12
 * Time: 9:28 AM
 */
public class MongoDBQueryEngine implements RyaQueryEngine<StatefulMongoDBRdfConfiguration> {

    private StatefulMongoDBRdfConfiguration configuration;
    private final MongoDBStorageStrategy<RyaStatement> strategy = new SimpleMongoDBStorageStrategy();

    @Override
    public void setConf(final StatefulMongoDBRdfConfiguration conf) {
        configuration = conf;
    }

    @Override
    public StatefulMongoDBRdfConfiguration getConf() {
        return configuration;
    }

    @Override
    public CloseableIteration<Entry<RyaStatement, BindingSet>, RyaDAOException> queryWithBindingSet(
            final SetMultimap<RyaStatement, BindingSet> stmts,
            final StatefulMongoDBRdfConfiguration conf) throws RyaDAOException {
        checkNotNull(stmts);
        checkNotNull(conf);

        final Multimap<RyaStatement, BindingSet> rangeMap = HashMultimap.create();

        //TODO: cannot span multiple tables here
        try {
            for (final Map.Entry<RyaStatement, BindingSet> stmtbs : stmts.entries()) {
                final RyaStatement stmt = stmtbs.getKey();
                final BindingSet bs = stmtbs.getValue();
                rangeMap.put(stmt, bs);
            }

            // TODO not sure what to do about regex ranges?
            final RyaStatementBindingSetCursorIterator iterator = new RyaStatementBindingSetCursorIterator(
                    getCollection(conf), rangeMap, strategy, conf.getAuthorizations());

            return iterator;
        } catch (final Exception e) {
            throw new RyaDAOException(e);
        }

    }

    private MongoCollection<Document> getCollection(final StatefulMongoDBRdfConfiguration conf) {
        final MongoDatabase db = conf.getMongoClient().getDatabase(conf.getMongoDBName());
        return db.getCollection(conf.getTriplesCollectionName());
    }

    @Override
    public void close() throws IOException {
    }
}