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
package org.apache.rya.mongodb;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaStatement.RyaStatementBuilder;
import org.apache.rya.api.persist.utils.RyaDAOHelper;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Integration tests the methods of {@link MongoDBQueryEngine}.
 */
public class MongoDBQueryEngineIT extends MongoRyaITBase {
    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    private RyaStatement getStatement(final String s, final String p, final String o) {
        final RyaStatementBuilder builder = new RyaStatementBuilder();
        if (s != null) {
            builder.setSubject(new RyaIRI(s));
        }
        if (p != null) {
            builder.setPredicate(new RyaIRI(p));
        }
        if (o != null) {
            builder.setObject(new RyaIRI(o));
        }
        return builder.build();
    }

    public int size(final CloseableIteration<?, ?> iter) throws Exception {
        int i = 0;
        while (iter.hasNext()) {
            i++;
            iter.next();
        }
        return i;
    }

    @Test
    public void statementQuery() throws Exception {
        final MongoDBRyaDAO dao = new MongoDBRyaDAO();
        try(final MongoDBQueryEngine engine =new MongoDBQueryEngine()) {
            engine.setConf(conf);

            // Add data.
            dao.setConf(conf);
            dao.init();
            dao.add(getStatement("u:a", "u:tt", "u:b"));
            dao.add(getStatement("u:a", "u:tt", "u:c"));

            final RyaStatement s = getStatement("u:a", null, null);
            assertEquals(2, size(RyaDAOHelper.query(dao.getQueryEngine(), s, conf)));
        } finally {
            dao.destroy();
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void batchbindingSetsQuery() throws Exception {
        final MongoDBRyaDAO dao = new MongoDBRyaDAO();
        try(final MongoDBQueryEngine engine =new MongoDBQueryEngine()) {
            engine.setConf(conf);

            // Add data.
            dao.setConf(conf);
            dao.init();
            dao.add(getStatement("u:a", "u:tt", "u:b"));
            dao.add(getStatement("u:a", "u:tt", "u:c"));

            // Run the test.
            final RyaStatement s1 = getStatement(null, null, "u:b");

            final MapBindingSet bs1 = new MapBindingSet();
            bs1.addBinding("foo", VF.createIRI("u:x"));

            final SetMultimap<RyaStatement, BindingSet> stmts1 = HashMultimap.create();
            stmts1.put(s1, bs1);
            assertEquals(1, size(engine.queryWithBindingSet(stmts1, conf)));


            final MapBindingSet bs2 = new MapBindingSet();
            bs2.addBinding("foo", VF.createIRI("u:y"));

            final RyaStatement s2 = getStatement(null, null, "u:c");

            final SetMultimap<RyaStatement, BindingSet> stmts2 = HashMultimap.create();
            stmts2.put(s1, bs1);
            stmts2.put(s2, bs2);
            assertEquals(2, size(engine.queryWithBindingSet(stmts2, conf)));


            final SetMultimap<RyaStatement, BindingSet> stmts3 = HashMultimap.create();
            stmts3.put(s1, bs1);
            stmts3.put(s2, bs2);
            stmts3.put(s2, bs1);
            stmts3.put(s1, bs2);
            assertEquals(4, size(engine.queryWithBindingSet(stmts3, conf)));
        } finally {
            dao.destroy();
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void bindingSetsQuery() throws Exception {
        final MongoDBRyaDAO dao = new MongoDBRyaDAO();
        try(final MongoDBQueryEngine engine =new MongoDBQueryEngine()) {
            engine.setConf(conf);

            // Add data.
            dao.setConf(conf);
            dao.init();
            dao.add(getStatement("u:a", "u:tt", "u:b"));
            dao.add(getStatement("u:a", "u:tt", "u:c"));

            // Run the test.
            final RyaStatement s = getStatement("u:a", null, null);

            final MapBindingSet bs1 = new MapBindingSet();
            bs1.addBinding("foo", VF.createIRI("u:x"));

            final SetMultimap<RyaStatement, BindingSet> stmts1 = HashMultimap.create();
            stmts1.put(s, bs1);
            assertEquals(2, size(engine.queryWithBindingSet(stmts1, conf)));


            final MapBindingSet bs2 = new MapBindingSet();
            bs2.addBinding("foo", VF.createIRI("u:y"));

            final SetMultimap<RyaStatement, BindingSet> stmts2 = HashMultimap.create();
            stmts2.put(s, bs1);
            stmts2.put(s, bs2);
            assertEquals(4, size(engine.queryWithBindingSet(stmts2, conf)));
        } finally {
            dao.destroy();
        }
    }
}