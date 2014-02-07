/*
 * Copyright (c) 2013. Jordan Williams
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jordanwilliams.heftydb.state;

import com.jordanwilliams.heftydb.table.Table;

import java.util.Collection;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Tables implements Iterable<Table> {

    private class ReadLockIterator implements Iterator<Table> {

        private final Iterator<Table> delegate;

        public ReadLockIterator(Iterator<Table> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            boolean hasNext = delegate.hasNext();
            if (!hasNext) {
                tableLock.readLock().unlock();
            }

            return hasNext;
        }

        @Override
        public Table next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            return delegate.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private final AtomicLong currentTableId = new AtomicLong();
    private final NavigableSet<Table> tables = new TreeSet<Table>();
    private final ReadWriteLock tableLock = new ReentrantReadWriteLock();

    public Tables(Collection<Table> initialTables) {
        this.tables.addAll(initialTables);
        this.currentTableId.set(tables.isEmpty() ? 0 : tables.last().id());
    }

    public long nextId() {
        return currentTableId.incrementAndGet();
    }

    public long currentId() {
        return currentTableId.get();
    }

    public Iterator<Table> all() {
        try {
            tableLock.readLock().lock();
            return new ReadLockIterator(tables.iterator());
        } finally {
            tableLock.readLock().unlock();
        }
    }

    public Iterator<Table> persistent() {
        try {
            tableLock.readLock().lock();
            SortedSet<Table> tableSnapshot = new TreeSet<Table>();

            for (Table table : tables) {
                if (table.isPersistent()) {
                    tableSnapshot.add(table);
                }
            }

            return new ReadLockIterator(tableSnapshot.iterator());
        } finally {
            tableLock.readLock().unlock();
        }
    }

    public void add(Table toAdd) {
        try {
            tableLock.writeLock().lock();
            tables.add(toAdd);
        } finally {
            tableLock.writeLock().unlock();
        }
    }

    public void remove(Table toRemove) {
        try {
            tableLock.writeLock().lock();
            tables.remove(toRemove);
        } finally {
            tableLock.writeLock().unlock();
        }
    }

    public void swap(Table toAdd, Table toRemove) {
        try {
            tableLock.writeLock().lock();
            tables.remove(toRemove);
            tables.add(toAdd);
        } finally {
            tableLock.writeLock().unlock();
        }
    }

    public int count() {
        return tables.size();
    }

    @Override
    public Iterator<Table> iterator() {
        return all();
    }
}
