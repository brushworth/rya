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

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class LiFoBlockingQueue implements BlockingQueue<Runnable> {

    LinkedBlockingDeque<Runnable> queue = new LinkedBlockingDeque<>();

    @Override
    public boolean add(Runnable runnable) {
        queue.addFirst(runnable);
        return true;
    }

    @Override
    public boolean offer(Runnable runnable) {
        return queue.offerFirst(runnable);
    }

    @Override
    public Runnable remove() {
        return queue.remove();
    }

    @Override
    public Runnable poll() {
        return queue.poll();
    }

    @Override
    public Runnable element() {
        return queue.element();
    }

    @Override
    public Runnable peek() {
        return queue.peek();
    }

    @Override
    public void put(Runnable runnable) throws InterruptedException {
        queue.putFirst(runnable);
    }

    @Override
    public boolean offer(Runnable runnable, long l, TimeUnit timeUnit) throws InterruptedException {
        return queue.offerFirst(runnable, l, timeUnit);
    }

    @Override
    public Runnable take() throws InterruptedException {
        return queue.take();
    }

    @Override
    public Runnable poll(long l, TimeUnit timeUnit) throws InterruptedException {
        return queue.poll(l, timeUnit);
    }

    @Override
    public int remainingCapacity() {
        return queue.remainingCapacity();
    }

    @Override
    public boolean remove(Object o) {
        return queue.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> collection) {
        return queue.containsAll(collection);
    }

    @Override
    public boolean addAll(Collection<? extends Runnable> collection) {
        return queue.addAll(collection);
    }

    @Override
    public boolean removeAll(Collection<?> collection) {
        return queue.removeAll(collection);
    }

    @Override
    public boolean retainAll(Collection<?> collection) {
        return queue.retainAll(collection);
    }

    @Override
    public void clear() {
        queue.clear();
    }

    @Override
    public int size() {
        return queue.size();
    }

    @Override
    public boolean isEmpty() {
        return queue.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return queue.contains(o);
    }

    @Override
    public Iterator<Runnable> iterator() {
        return queue.iterator();
    }

    @Override
    public Object[] toArray() {
        return queue.toArray();
    }

    @Override
    public <T> T[] toArray(T[] ts) {
        return queue.toArray(ts);
    }

    @Override
    public int drainTo(Collection<? super Runnable> collection) {
        return queue.drainTo(collection);
    }

    @Override
    public int drainTo(Collection<? super Runnable> collection, int i) {
        return queue.drainTo(collection, i);
    }

}
