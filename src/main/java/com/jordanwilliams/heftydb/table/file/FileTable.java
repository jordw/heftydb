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
import com.jordanwilliams.heftydb.read.VersionedRecordIterator;
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

    private class TableIterator implements Iterator<Record> {

        private final boolean ascending;
        private final IterationDirection iterationDirection;
        private final Queue<Record> nextRecord = new LinkedList<Record>();
        private final Iterator<Long> blockOffsets;

        private Iterator<Record> recordIterator;
        private RecordBlock recordBlock;

        public TableIterator(Key startKey, IterationDirection iterationDirection) {
            try {
                this.iterationDirection = iterationDirection;
                this.ascending = iterationDirection.equals(IterationDirection.ASCENDING);
                this.blockOffsets = blockOffsets(startKey);

                if (startKey != null) {
                    long blockOffset = index.recordBlockOffset(startKey);
                    this.recordBlock = readRecordBlock(blockOffset, false);
                    this.recordIterator = recordBlock.iteratorFrom(startKey, iterationDirection);
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

                if (!blockOffsets.hasNext()) {
                    return false;
                }

                long blockOffset = blockOffsets.next();
                this.recordBlock = readRecordBlock(blockOffset, false);
                this.recordIterator = recordBlock.iterator(iterationDirection);

                return true;
            } catch (IOException e) {
                recordBlock.memory().release();
                throw new RuntimeException(e);
            }
        }

        private Iterator<Long> blockOffsets(Key startKey) throws IOException {
            if (startKey == null) {
                return ascending ? recordBlockOffsets.iterator() : recordBlockOffsets.descendingIterator();
            }

            long startingBlockOffset = index.recordBlockOffset(startKey);

            return ascending ? recordBlockOffsets.tailSet(startingBlockOffset, false).iterator() : recordBlockOffsets.headSet(startingBlockOffset, false).descendingIterator();
        }
    }

    private final long tableId;
    private final NavigableSet<Long> recordBlockOffsets;
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
        this.recordBlockOffsets = readRecordBlockOffsets();
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
            long blockOffset = index.recordBlockOffset(key);

            if (blockOffset < 0) {
                return null;
            }

            RecordBlock recordBlock = readRecordBlock(blockOffset);
            return recordBlock.get(key);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterator<Record> iterator(IterationDirection direction, long maxSnapshotId) {
        return new VersionedRecordIterator(maxSnapshotId, new TableIterator(direction));
    }

    @Override
    public Iterator<Record> iteratorFrom(Key key, IterationDirection direction, long maxSnapshotId) {
        return new VersionedRecordIterator(maxSnapshotId, new TableIterator(key, direction));
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

    private RecordBlock readRecordBlock(long offset) throws IOException {
        return readRecordBlock(offset, true);
    }

    private RecordBlock readRecordBlock(long offset, boolean shouldCache) throws IOException {
        RecordBlock recordBlock = recordCache.get(tableId, offset);

        if (recordBlock == null) {
            int recordBlockSize = tableFile.readInt(offset);
            Memory recordBlockMemory = Memory.allocate(recordBlockSize);
            ByteBuffer recordBlockBuffer = recordBlockMemory.directBuffer();
            tableFile.read(recordBlockBuffer, offset + Sizes.INT_SIZE);
            recordBlockBuffer.rewind();
            recordBlock = new RecordBlock(new ByteMap(recordBlockMemory));

            if (shouldCache) {
                recordCache.put(tableId, offset, recordBlock);
            }
        }

        return recordBlock;
    }

    private NavigableSet<Long> readRecordBlockOffsets() throws IOException {
        NavigableSet<Long> recordBlockOffsets = new TreeSet<Long>();
        long fileOffset = tableFile.size() - Sizes.INT_SIZE;
        int offsetCount = tableFile.readInt(fileOffset);
        fileOffset -= Sizes.LONG_SIZE;

        for (int i = 0; i < offsetCount; i++) {
            recordBlockOffsets.add(tableFile.readLong(fileOffset));
            fileOffset -= Sizes.LONG_SIZE;
        }

        return recordBlockOffsets;
    }

    public static FileTable open(long tableId, Paths paths, RecordBlock.Cache recordCache, IndexBlock.Cache indexCache) throws IOException {
        return new FileTable(tableId, paths, recordCache, indexCache);
    }
}
