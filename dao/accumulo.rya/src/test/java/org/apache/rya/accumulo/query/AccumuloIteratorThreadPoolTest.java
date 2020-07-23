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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 *
 */
public class AccumuloIteratorThreadPoolTest {

    private static final Log logger = LogFactory.getLog(AccumuloIteratorThreadPoolTest.class);

    private static final long SLEEP_TIME = 50; // milliseconds

    @Before
    public void before() {
        AccumuloIteratorThreadPool.reset();
    }

    @After
    public void after() {
        AccumuloIteratorThreadPool.reset();
    }

    @Test
    public void testIncrementingDecrementing() {
        final int threads = 1000;

        final AccumuloIteratorThreadPool pool1 = new AccumuloIteratorThreadPool(threads);
        assertEquals(500, pool1.getCurrentNumThreadsPerServer());
        assertTrue(pool1.isIdle());
        pool1.close();
        assertEquals(0, pool1.getCurrentNumThreadsPerServer());
        assertTrue(pool1.isIdle());

        final AccumuloIteratorThreadPool pool2 = new AccumuloIteratorThreadPool(threads);
        assertEquals(500, pool2.getCurrentNumThreadsPerServer());
        assertTrue(pool2.isIdle());
        final AccumuloIteratorThreadPool pool3 = new AccumuloIteratorThreadPool(threads);
        assertEquals(667, pool3.getCurrentNumThreadsPerServer());
        assertTrue(pool3.isIdle());
        pool2.close();
        pool3.close();
    }

    @Test
    public void testMultiplePools() {
        final int threads = 1000;

        final AccumuloIteratorThreadPool[] pools = new AccumuloIteratorThreadPool[20];
        int lastNumThreads = 0;
        for (int i = 0; i < pools.length; i++) {
            pools[i] = new AccumuloIteratorThreadPool(threads);
            assertTrue(lastNumThreads + " < " + pools[i].getCurrentNumThreadsPerServer(), lastNumThreads < pools[i].getCurrentNumThreadsPerServer());
            lastNumThreads = pools[i].getCurrentNumThreadsPerServer();
        }

        lastNumThreads = threads;
        for (int i = 0; i < pools.length; i++) {
            pools[i].close();
            assertTrue(lastNumThreads + " > " + pools[i].getCurrentNumThreadsPerServer(), lastNumThreads > pools[i].getCurrentNumThreadsPerServer());
            lastNumThreads = pools[i].getCurrentNumThreadsPerServer();
        }
    }

    @Test
    public void testThreadPool() throws InterruptedException {
        final int threads = 6;
        final AccumuloIteratorThreadPool pool = new AccumuloIteratorThreadPool(threads);
        assertEquals(3, pool.getCurrentNumThreadsPerServer());
        assertTrue(pool.isIdle());

        pool.execute("server1", new TestSleepRunnable());
        assertFalse(pool.isIdle());
        pool.execute("server2", new TestSleepRunnable());
        pool.execute("server2", new TestSleepRunnable());
        assertFalse(pool.isIdle());
        pool.execute("server3", new TestSleepRunnable());
        pool.execute("server3", new TestSleepRunnable());
        pool.execute("server3", new TestSleepRunnable());
        assertFalse(pool.isIdle());

        // Closing doesn't stop the threads
        pool.close();
        assertEquals(0, pool.getCurrentNumThreadsPerServer());
        assertFalse(pool.isIdle());

        // Wait for threads to finish
        Thread.sleep(SLEEP_TIME * 2);
        assertTrue(pool.isIdle());
    }

    @Test
    public void testMultipleThreadPools() throws InterruptedException {
        final int threads = 4;

        // The thread pool will be half the total number, so only 2 threads will run at a time, but we have 4 threads to do
        final AccumuloIteratorThreadPool pool = new AccumuloIteratorThreadPool(threads);
        for (int i = 0; i < threads; i++) {
            pool.execute("server1", new TestSleepRunnable());
        }

        // We cause one set of threads to queue, so you need twice sleep time
        assertFalse(pool.isIdle());
        Thread.sleep(SLEEP_TIME);
        assertFalse(pool.isIdle());
        Thread.sleep(SLEEP_TIME * 2);
        assertTrue(pool.isIdle());

        pool.close();
    }

    @Test
    public void testLiLoThreadPools() throws InterruptedException {
        final int threads = 10;
        final String tablet = "server1";

        // Simulate the first query
        final AccumuloIteratorThreadPool pool1 = new AccumuloIteratorThreadPool(threads);
        TestForeverRunnable[] runnable1 = new TestForeverRunnable[threads];
        for (int i = 0; i < threads; i++) {
            runnable1[i] = new TestForeverRunnable("runnable1["+i+"]");
            pool1.execute(tablet, runnable1[i]);
        }

        // Simulate the second query
        final AccumuloIteratorThreadPool pool2 = new AccumuloIteratorThreadPool(threads);
        TestForeverRunnable[] runnable2 = new TestForeverRunnable[threads];
        for (int i = 0; i < threads; i++) {
            runnable2[i] = new TestForeverRunnable("runnable2["+i+"]");
            pool2.execute(tablet, runnable2[i]);
        }

        // Simulate the third query
        final AccumuloIteratorThreadPool pool3 = new AccumuloIteratorThreadPool(threads);
        TestForeverRunnable[] runnable3 = new TestForeverRunnable[threads];
        for (int i = 0; i < threads; i++) {
            runnable3[i] = new TestForeverRunnable("runnable3["+i+"]");
            pool3.execute(tablet, runnable3[i]);
        }

        Thread.sleep(SLEEP_TIME);

        List<Runnable> actual = pool1.getTaskOrder(tablet);

        // Each query should have at least one thread started i.e. not all queued
        assertFalse(actual.containsAll(Arrays.asList(runnable1)));
        assertFalse(actual.containsAll(Arrays.asList(runnable2)));
        assertFalse(actual.containsAll(Arrays.asList(runnable3)));

        pool1.close();
        pool2.close();
        pool3.close();
    }

    private static class TestSleepRunnable implements Runnable {

        @Override
        public void run() {
            try {
                Thread.sleep(SLEEP_TIME);
            } catch (InterruptedException e) {
                fail(e.toString());
            }
        }

    }

    private static class TestForeverRunnable implements Runnable {

        private final String name;

        public TestForeverRunnable(String name) {
            this.name = name;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    Thread.sleep(SLEEP_TIME);
                }
            } catch (InterruptedException e) {
                fail(e.toString());
            }
        }

        @Override
        public String toString() {
            return name;
        }

    }

}