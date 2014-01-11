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
import com.jordanwilliams.heftydb.record.Key;
import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.state.Paths;
import com.jordanwilliams.heftydb.table.Table;
import com.jordanwilliams.heftydb.util.Sizes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class FileTable implements Table {

    private static class TableIterator implements Iterator<Record> {

        private final IterationDirection iterationDirection;
        private final Key startKey;

        private final Queue<Iterator<Record>> nextRecordIterator = new LinkedList<Iterator<Record>>();
        private final Queue<RecordBlock> nextRecordBlock = new LinkedList<RecordBlock>();

        private TableIterator(Key startKey, IterationDirection iterationDirection) {
            this.startKey = startKey;
            this.iterationDirection = iterationDirection;
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Record next() {
            return null;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private final long tableId;
    private final Index index;
    private final Filter filter;
    private final MetaTable metaTable;
    private final RecordBlock.Cache recordCache;
    private final DataFile tableFile;

    private FileTable(long tableId, Paths paths, RecordBlock.Cache recordCache, IndexBlock.Cache indexCache) throws
            IOException {
        this.tableId = tableId;
        this.recordCache = recordCache;
        this.index = Index.open(tableId, paths, indexCache);
        this.filter = Filter.open(tableId, paths);
        this.tableFile = MutableDataFile.open(paths.tablePath(tableId));
        this.metaTable = MetaTable.open(tableId, paths);
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
    public Record get(Key key, long snapshotId) {
        try {
            long blockOffset = index.recordBlockOffset(key, snapshotId);

            if (blockOffset < 0){
                return null;
            }

            RecordBlock recordBlock = readRecordBlock(blockOffset);
            return recordBlock.get(key, snapshotId);
        } catch (IOException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterator<Record> iterator(IterationDirection direction, long snapshotId) {
        return null;
    }

    @Override
    public Iterator<Record> iteratorFrom(Key key, IterationDirection direction, long sn) {
        return null;
    }

    @Override
    public long recordCount() {
        return metaTable.recordCount();
    }

    @Override
    public long sizeBytes() {
        return metaTable.sizeBytes();
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
        return null;
    }

    private RecordBlock readRecordBlock(long offset) throws IOException {
        RecordBlock recordBlock = recordCache.get(tableId, offset);

        if (recordBlock == null){
            int recordBlockSize = tableFile.readInt(offset);
            Memory recordBlockMemory = Memory.allocate(recordBlockSize);
            ByteBuffer recordBlockBuffer = recordBlockMemory.directBuffer();
            tableFile.read(recordBlockBuffer, offset + Sizes.INT_SIZE);
            recordBlockBuffer.rewind();
            recordBlock = new RecordBlock(new ByteMap(recordBlockMemory));
            recordCache.put(tableId, offset, recordBlock);
        }

        return recordBlock;
    }

    public static FileTable open(long tableId, Paths paths, RecordBlock.Cache recordCache,
                                 IndexBlock.Cache indexCache)
            throws
            IOException {
        return new FileTable(tableId, paths, recordCache, indexCache);
    }
}
