/*
 * Copyright (c) 2014. Jordan Williams
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Tables implements Iterable<Table> {

    public interface ChangeHandler {
        public void changed();
    }

    private final AtomicLong currentTableId = new AtomicLong();
    private final NavigableMap<Long, Table> tables = new TreeMap<Long, Table>();
    private final ReadWriteLock tableLock = new ReentrantReadWriteLock();
    private final List<ChangeHandler> changeHandlers = new ArrayList<ChangeHandler>();

    public Tables(Collection<Table> initialTables) {
        for (Table table : initialTables){
            tables.put(table.id(), table);
        }

        this.currentTableId.set(tables.isEmpty() ? 0 : tables.lastEntry().getValue().id());
    }

    public synchronized void addChangeHandler(ChangeHandler changeHandler) {
        changeHandlers.add(changeHandler);
    }

    public synchronized void removeChangeHandler(ChangeHandler changeHandler) {
        changeHandlers.remove(changeHandler);
    }

    public long nextId() {
        return currentTableId.incrementAndGet();
    }

    public long currentId() {
        return currentTableId.get();
    }

    public void readLock() {
        tableLock.readLock().lock();
    }

    public void readUnlock() {
        tableLock.readLock().unlock();
    }

    public Table get(long tableId) {
        try {
            tableLock.readLock().lock();
            Table table = tables.get(tableId);
            tableLock.readLock().unlock();
            return table;
        } finally {
            tableLock.readLock().unlock();
        }
    }

    public void add(Table toAdd) {
        try {
            tableLock.writeLock().lock();
            tables.put(toAdd.id(), toAdd);
            notifyChanged();
        } finally {
            tableLock.writeLock().unlock();
        }
    }

    public void removeAll(List<Table> toRemove) {
        try {
            tableLock.writeLock().lock();
            for (Table table : toRemove) {
                tables.remove(table.id());
            }
            notifyChanged();
        } finally {
            tableLock.writeLock().unlock();
        }
    }

    public void remove(Table toRemove) {
        try {
            tableLock.writeLock().lock();
            tables.remove(toRemove.id());
            notifyChanged();
        } finally {
            tableLock.writeLock().unlock();
        }
    }

    public void swap(Table toAdd, Table toRemove) {
        try {
            tableLock.writeLock().lock();
            tables.remove(toRemove.id());
            tables.put(toAdd.id(), toAdd);
            notifyChanged();
        } finally {
            tableLock.writeLock().unlock();
        }
    }

    public int count() {
        return tables.size();
    }

    @Override
    public Iterator<Table> iterator() {
        return tables.values().iterator();
    }

    private synchronized void notifyChanged() {
        for (ChangeHandler changeHandler : changeHandlers) {
            changeHandler.changed();
        }
    }
}
