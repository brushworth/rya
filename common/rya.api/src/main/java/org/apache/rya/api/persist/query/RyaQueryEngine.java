package org.apache.rya.api.persist.query;

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

import com.google.common.collect.SetMultimap;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.persist.RyaConfigured;
import org.apache.rya.api.persist.RyaDAOException;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.BindingSet;

import java.io.Closeable;
import java.util.Map;

/**
 * Rya Query Engine to perform queries against the Rya triple store.
 * <p/>
 * Date: 7/17/12
 * Time: 8:25 AM
 */
public interface RyaQueryEngine<C extends RdfCloudTripleStoreConfiguration> extends RyaConfigured<C>, Closeable {

    /**
     * Batch query
     *
     * @param stmts
     * @param conf
     * @return
     * @throws RyaDAOException
     */
    CloseableIteration<Map.Entry<RyaStatement, BindingSet>, RyaDAOException>
    queryWithBindingSet(SetMultimap<RyaStatement, BindingSet> stmts, C conf) throws RyaDAOException;

}