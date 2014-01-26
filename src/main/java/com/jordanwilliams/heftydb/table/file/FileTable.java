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

package com.jordanwilliams.heftydb.table.file;

import com.jordanwilliams.heftydb.io.DataFile;
import com.jordanwilliams.heftydb.io.MutableDataFile;
import com.jordanwilliams.heftydb.offheap.ByteMap;
import com.jordanwilliams.heftydb.offheap.Memory;
import com.jordanwilliams.heftydb.read.LatestRecordIterator;
import com.jordanwilliams.heftydb.record.Key;
import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.state.Paths;
import com.jordanwilliams.heftydb.table.Table;
import com.jordanwilliams.heftydb.util.Sizes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NavigableSet;
import java.util.Queue;
import java.util.TreeSet;

public class FileTable implements Table {

    public static class RecordBlockDescriptor implements Comparable<RecordBlockDescriptor> {

        public static int SIZE = 12;

        private final long offset;
        private final int size;

        public RecordBlockDescriptor(long offset, int size) {
            this.offset = offset;
            this.size = size;
        }

        public long offset() {
            return offset;
        }

        public int size() {
            return size;
        }

        @Override
        public int compareTo(RecordBlockDescriptor o) {
            return Long.compare(offset, o.offset);
        }

        @Override
        public String toString() {
            return "RecordBlockDescriptor{" +
                    "offset=" + offset +
                    ", size=" + size +
                    '}';
        }
    }

    private class TableIterator implements Iterator<Record> {

        private final boolean ascending;
        private final Queue<Record> nextRecord = new LinkedList<Record>();
        private final Iterator<RecordBlockDescriptor> blockDescriptors;

        private Iterator<Record> recordIterator;
        private RecordBlock recordBlock;

        public TableIterator(Key startKey, IterationDirection iterationDirection) {
            try {
                this.ascending = iterationDirection.equals(IterationDirection.ASCENDING);
                this.blockDescriptors = blockDescriptors(startKey);

                if (startKey != null) {
                    IndexRecord indexRecord = index.get(startKey);
                    this.recordBlock = readRecordBlock(indexRecord.blockOffset(), indexRecord.blockSize(), false);
                    this.recordIterator = ascending ? recordBlock.ascendingIterator(startKey) : recordBlock .descendingIterator(startKey);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public TableIterator(IterationDirection iterationDirection) {
            this(null, iterationDirection);
        }

        @Override
        public boolean hasNext() {
            if (!nextRecord.isEmpty()) {
                return true;
            }

            if (recordIterator == null || !recordIterator.hasNext()) {
                if (!nextRecordBlock()) {
                    return false;
                }
            }

            if (!recordIterator.hasNext()) {
                return false;
            }

            nextRecord.add(recordIterator.next());
            return true;
        }

        @Override
        public Record next() {
            if (nextRecord.isEmpty()) {
                hasNext();
            }

            return nextRecord.poll();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private boolean nextRecordBlock() {
            try {
                if (recordBlock != null) {
                    recordBlock.memory().release();
                }

                if (!blockDescriptors.hasNext()) {
                    return false;
                }

                RecordBlockDescriptor descriptor = blockDescriptors.next();
                this.recordBlock = readRecordBlock(descriptor.offset, descriptor.size, false);
                this.recordIterator = ascending ? recordBlock.ascendingIterator() : recordBlock.descendingIterator();

                return true;
            } catch (IOException e) {
                recordBlock.memory().release();
                throw new RuntimeException(e);
            }
        }

        private Iterator<RecordBlockDescriptor> blockDescriptors(Key startKey) throws IOException {
            if (startKey == null) {
                return ascending ? recordBlockDescriptors.iterator() : recordBlockDescriptors.descendingIterator();
            }

            IndexRecord indexRecord = index.get(startKey);
            RecordBlockDescriptor descriptor = new RecordBlockDescriptor(indexRecord.blockOffset(), indexRecord.blockSize());

            return ascending ? recordBlockDescriptors.tailSet(descriptor, false).iterator() :
                               recordBlockDescriptors.headSet(descriptor, false).descendingIterator();
        }
    }

    private final long tableId;
    private final NavigableSet<RecordBlockDescriptor> recordBlockDescriptors;
    private final Index index;
    private final Filter filter;
    private final MetaTable metaTable;
    private final RecordBlock.Cache recordCache;
    private final DataFile tableFile;

    private FileTable(long tableId, Paths paths, RecordBlock.Cache recordCache, IndexBlock.Cache indexCache) throws IOException {
        this.tableId = tableId;
        this.recordCache = recordCache;
        this.index = Index.open(tableId, paths, indexCache);
        this.filter = Filter.open(tableId, paths);
        this.tableFile = MutableDataFile.open(paths.tablePath(tableId));
        this.metaTable = MetaTable.open(tableId, paths);
        this.recordBlockDescriptors = readRecordBlockDescriptors();
    }

    @Override
    public long id() {
        return tableId;
    }

    @Override
    public boolean mightContain(Key key) {
        return filter.mightContain(key);
    }

    @Override
    public Record get(Key key) {
        try {
            IndexRecord indexRecord = index.get(key);

            if (indexRecord.blockOffset() < 0) {
                return null;
            }

            RecordBlock recordBlock = readRecordBlock(indexRecord.blockOffset(), indexRecord.blockSize());
            return recordBlock.get(key);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterator<Record> ascendingIterator(long snapshotId) {
        return new LatestRecordIterator(snapshotId, new TableIterator(IterationDirection.ASCENDING));
    }

    @Override
    public Iterator<Record> descendingIterator(long snapshotId) {
        return new LatestRecordIterator(snapshotId, new TableIterator(IterationDirection.DESCENDING));
    }

    @Override
    public Iterator<Record> ascendingIterator(Key key, long snapshotId) {
        return new LatestRecordIterator(snapshotId, new TableIterator(key, IterationDirection.ASCENDING));
    }

    @Override
    public Iterator<Record> descendingIterator(Key key, long snapshotId) {
        return new LatestRecordIterator(snapshotId, new TableIterator(key, IterationDirection.DESCENDING));
    }

    @Override
    public long recordCount() {
        return metaTable.recordCount();
    }

    @Override
    public long size() {
        return metaTable.size();
    }

    @Override
    public int level() {
        return metaTable.level();
    }

    @Override
    public boolean isPersistent() {
        return true;
    }

    @Override
    public Iterator<Record> iterator() {
        return new TableIterator(IterationDirection.ASCENDING);
    }

    private RecordBlock readRecordBlock(long offset, int size) throws IOException {
        return readRecordBlock(offset, size, true);
    }

    private RecordBlock readRecordBlock(long offset, int size, boolean shouldCache) throws IOException {
        RecordBlock recordBlock = recordCache.get(tableId, offset);

        if (recordBlock == null) {
            Memory recordBlockMemory = Memory.allocate(size);

            try {
                ByteBuffer recordBlockBuffer = recordBlockMemory.directBuffer();
                tableFile.read(recordBlockBuffer, offset);
                recordBlockBuffer.rewind();
                recordBlock = new RecordBlock(new ByteMap(recordBlockMemory));

                if (shouldCache) {
                    recordCache.put(tableId, offset, recordBlock);
                }
            } catch (IOException e){
                recordBlockMemory.release();
                throw e;
            }
        }

        return recordBlock;
    }

    private NavigableSet<RecordBlockDescriptor> readRecordBlockDescriptors() throws IOException {
        NavigableSet<RecordBlockDescriptor> recordBlockDescriptors = new TreeSet<RecordBlockDescriptor>();
        long fileOffset = tableFile.size() - Sizes.INT_SIZE;
        int descriptorCount = tableFile.readInt(fileOffset);

        ByteBuffer descriptorBuffer = ByteBuffer.allocate(descriptorCount * RecordBlockDescriptor.SIZE);

        tableFile.read(descriptorBuffer, fileOffset - descriptorBuffer.capacity());
        descriptorBuffer.rewind();

        for (int i = 0; i < descriptorCount; i++){
            long offset = descriptorBuffer.getLong();
            int size = descriptorBuffer.getInt();
            recordBlockDescriptors.add(new RecordBlockDescriptor(offset, size));
        }

        return recordBlockDescriptors;
    }

    public static FileTable open(long tableId, Paths paths, RecordBlock.Cache recordCache, IndexBlock.Cache indexCache) throws IOException {
        return new FileTable(tableId, paths, recordCache, indexCache);
    }
}
