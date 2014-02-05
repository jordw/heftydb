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

import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.index.Index;
import com.jordanwilliams.heftydb.index.IndexBlock;
import com.jordanwilliams.heftydb.index.IndexRecord;
import com.jordanwilliams.heftydb.io.DataFile;
import com.jordanwilliams.heftydb.io.MutableDataFile;
import com.jordanwilliams.heftydb.offheap.ByteMap;
import com.jordanwilliams.heftydb.offheap.Memory;
import com.jordanwilliams.heftydb.read.LatestTupleIterator;
import com.jordanwilliams.heftydb.data.Key;
import com.jordanwilliams.heftydb.state.Paths;
import com.jordanwilliams.heftydb.table.Table;
import com.jordanwilliams.heftydb.util.Sizes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class FileTable implements Table {

    private class AscendingBlockIterator implements Iterator<DataBlock> {

        private final long maxOffset;
        private long fileOffset = 0;

        public AscendingBlockIterator(long startOffset) {
            this.fileOffset = startOffset;
            this.maxOffset = fileSize - TableTrailer.SIZE - Sizes.LONG_SIZE;
        }

        @Override
        public boolean hasNext() {
            return fileOffset < maxOffset;
        }

        @Override
        public DataBlock next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            try {
                int nextBlockSize = tableFile.readInt(fileOffset);
                long nextBlockOffset = fileOffset + Sizes.INT_SIZE;

                fileOffset += Sizes.INT_SIZE;
                fileOffset += nextBlockSize;
                fileOffset += Sizes.INT_SIZE;

                return readRecordBlock(nextBlockOffset, nextBlockSize);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private class DescendingBlockIterator implements Iterator<DataBlock> {

        private long fileOffset;

        public DescendingBlockIterator(long startOffset) {
            this.fileOffset = startOffset;
        }

        @Override
        public boolean hasNext() {
            return fileOffset >= 0;
        }

        @Override
        public DataBlock next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            try {
                int nextBlockSize = tableFile.readInt(fileOffset);
                long nextBlockOffset = fileOffset - nextBlockSize;

                fileOffset -= Sizes.INT_SIZE;
                fileOffset -= nextBlockSize;
                fileOffset -= Sizes.INT_SIZE;

                return readRecordBlock(nextBlockOffset, nextBlockSize);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private class AscendingIterator implements Iterator<Tuple> {

        protected final Iterator<DataBlock> recordBlockIterator;
        protected Iterator<Tuple> recordIterator;
        protected DataBlock dataBlock;

        private AscendingIterator(Iterator<DataBlock> recordBlockIterator, Iterator<Tuple> startIterator,
                                  DataBlock startDataBlock) {
            this.recordBlockIterator = recordBlockIterator;
            this.recordIterator = startIterator;
            this.dataBlock = startDataBlock;
        }

        private AscendingIterator(Iterator<DataBlock> recordBlockIterator) {
            this.recordBlockIterator = recordBlockIterator;
        }

        @Override
        public boolean hasNext() {
            try {
                if (recordIterator == null || !recordIterator.hasNext()) {
                    if (!nextRecordBlock()) {
                        return false;
                    }
                }

                if (recordIterator == null || !recordIterator.hasNext()) {
                    return false;
                }

                return true;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Tuple next() {
            if (recordIterator == null || !recordIterator.hasNext()) {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
            }

            return recordIterator.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        protected boolean nextRecordBlock() throws IOException {
            if (dataBlock != null) {
                dataBlock.memory().release();
            }

            if (!recordBlockIterator.hasNext()) {
                return false;
            }

            dataBlock = recordBlockIterator.next();
            recordIterator = dataBlock.ascendingIterator();

            return true;
        }
    }

    private class DescendingIterator extends AscendingIterator {

        private DescendingIterator(Iterator<DataBlock> recordBlockIterator, Iterator<Tuple> startIterator,
                                   DataBlock startDataBlock) {
            super(recordBlockIterator, startIterator, startDataBlock);
        }

        private DescendingIterator(Iterator<DataBlock> recordBlockIterator) {
            super(recordBlockIterator);
        }

        @Override
        protected boolean nextRecordBlock() throws IOException {
            if (dataBlock != null) {
                dataBlock.memory().release();
            }

            if (!recordBlockIterator.hasNext()) {
                return false;
            }

            dataBlock = recordBlockIterator.next();
            recordIterator = dataBlock.descendingIterator();

            return true;
        }
    }

    private final long tableId;
    private final long fileSize;
    private final Index index;
    private final TableBloomFilter tableBloomFilter;
    private final TableTrailer trailer;
    private final DataBlock.Cache recordCache;
    private final DataFile tableFile;

    private FileTable(long tableId, Index index, TableBloomFilter tableBloomFilter, DataFile tableFile,
                      TableTrailer trailer, DataBlock.Cache recordCache) throws IOException {
        this.tableId = tableId;
        this.recordCache = recordCache;
        this.index = index;
        this.tableBloomFilter = tableBloomFilter;
        this.tableFile = tableFile;
        this.trailer = trailer;
        this.fileSize = tableFile.size();
    }

    @Override
    public boolean mightContain(Key key) {
        return tableBloomFilter.mightContain(key);
    }

    @Override
    public Tuple get(Key key) {
        try {
            IndexRecord indexRecord = index.get(key);

            if (indexRecord == null) {
                return null;
            }

            DataBlock dataBlock = getRecordBlock(indexRecord.blockOffset(), indexRecord.blockSize());
            return dataBlock.get(key);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterator<Tuple> ascendingIterator(long snapshotId) {
        return new LatestTupleIterator(snapshotId, new AscendingIterator(new AscendingBlockIterator(0)));
    }

    @Override
    public Iterator<Tuple> descendingIterator(long snapshotId) {
        try {
            long startOffset = tableFile.size() - TableTrailer.SIZE - Sizes.INT_SIZE;
            return new LatestTupleIterator(snapshotId, new DescendingIterator(new DescendingBlockIterator
                    (startOffset)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterator<Tuple> ascendingIterator(Key key, long snapshotId) {
        try {
            IndexRecord indexRecord = index.get(key);
            DataBlock startDataBlock = readRecordBlock(indexRecord.blockOffset(), indexRecord.blockSize());
            Iterator<Tuple> startRecordIterator = startDataBlock.ascendingIterator(key);
            long nextBlockOffset = indexRecord.blockOffset() + indexRecord.blockSize() + Sizes.INT_SIZE;
            return new LatestTupleIterator(snapshotId, new AscendingIterator(new AscendingBlockIterator
                    (nextBlockOffset), startRecordIterator,
                    startDataBlock));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterator<Tuple> descendingIterator(Key key, long snapshotId) {
        try {
            IndexRecord indexRecord = index.get(key);
            DataBlock startDataBlock = readRecordBlock(indexRecord.blockOffset(), indexRecord.blockSize());
            Iterator<Tuple> startRecordIterator = startDataBlock.descendingIterator(key);
            long nextBlockOffset = indexRecord.blockOffset() - Sizes.LONG_SIZE;
            return new LatestTupleIterator(snapshotId, new DescendingIterator(new DescendingBlockIterator
                    (nextBlockOffset), startRecordIterator, startDataBlock));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long id() {
        return tableId;
    }

    @Override
    public long recordCount() {
        return trailer.recordCount();
    }

    @Override
    public long size() {
        return fileSize;
    }

    @Override
    public int level() {
        return trailer.level();
    }

    @Override
    public long maxSnapshotId() {
        return trailer.maxSnapshotId();
    }

    @Override
    public void close() {
        try {
            index.close();
            tableFile.close();
            tableBloomFilter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isPersistent() {
        return true;
    }

    @Override
    public Iterator<Tuple> iterator() {
        return new AscendingIterator(new AscendingBlockIterator(0));
    }

    @Override
    public int compareTo(Table o) {
        return Long.compare(tableId, o.id());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FileTable fileTable = (FileTable) o;

        if (tableId != fileTable.tableId) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return (int) (tableId ^ (tableId >>> 32));
    }

    private DataBlock getRecordBlock(long offset, int size) throws IOException {
        DataBlock dataBlock = recordCache.get(tableId, offset);

        if (dataBlock == null) {
            dataBlock = readRecordBlock(offset, size);
            recordCache.put(tableId, offset, dataBlock);
        }

        return dataBlock;
    }

    private DataBlock readRecordBlock(long offset, int size) throws IOException {
        Memory recordBlockMemory = Memory.allocate(size);

        try {
            ByteBuffer recordBlockBuffer = recordBlockMemory.directBuffer();
            tableFile.read(recordBlockBuffer, offset);
            recordBlockBuffer.rewind();
            return new DataBlock(new ByteMap(recordBlockMemory));
        } catch (IOException e) {
            recordBlockMemory.release();
            throw e;
        }
    }

    public static FileTable open(long tableId, Paths paths, DataBlock.Cache recordCache,
                                 IndexBlock.Cache indexCache) throws IOException {
        Index index = Index.open(tableId, paths, indexCache);
        TableBloomFilter tableBloomFilter = TableBloomFilter.read(tableId, paths);
        DataFile tableFile = MutableDataFile.open(paths.tablePath(tableId));
        TableTrailer trailer = TableTrailer.read(tableFile);
        return new FileTable(tableId, index, tableBloomFilter, tableFile, trailer, recordCache);
    }
}
