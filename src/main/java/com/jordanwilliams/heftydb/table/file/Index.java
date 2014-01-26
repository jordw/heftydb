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

package com.jordanwilliams.heftydb.table.file;

import com.jordanwilliams.heftydb.io.DataFile;
import com.jordanwilliams.heftydb.io.MutableDataFile;
import com.jordanwilliams.heftydb.offheap.ByteMap;
import com.jordanwilliams.heftydb.offheap.Memory;
import com.jordanwilliams.heftydb.record.Key;
import com.jordanwilliams.heftydb.state.Paths;
import com.jordanwilliams.heftydb.util.Sizes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.TreeSet;

public class Index {

    public static class LeafBlock implements Comparable<LeafBlock> {

        public static int SIZE = 12;

        private final long offset;
        private final int size;

        public LeafBlock(long offset, int size) {
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
        public int compareTo(LeafBlock o) {
            return Long.compare(offset, o.offset);
        }

        @Override
        public String toString() {
            return "LeafBlock{" +
                    "offset=" + offset +
                    ", size=" + size +
                    '}';
        }
    }

    private class IndexBlockIterator implements Iterator<IndexBlock>  {

        private final Iterator<LeafBlock> leafBlockIterator;

        private IndexBlockIterator(Iterator<LeafBlock> leafBlockIterator) {
            this.leafBlockIterator = leafBlockIterator;
        }

        @Override
        public boolean hasNext() {
            return leafBlockIterator.hasNext();
        }

        @Override
        public IndexBlock next() {
            try {
                LeafBlock nextLeafBlock = leafBlockIterator.next();
                return readIndexBlock(nextLeafBlock.offset(), nextLeafBlock.size());
            } catch (IOException e){
                throw new RuntimeException(e);
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private class AscendingIterator implements Iterator<IndexRecord> {

        protected final Iterator<IndexBlock> indexBlockIterator;
        protected IndexBlock indexBlock;
        protected Iterator<IndexRecord> indexRecordIterator;

        private AscendingIterator(Iterator<IndexBlock> indexBlockIterator, Iterator<IndexRecord> startIterator){
            this.indexBlockIterator = indexBlockIterator;
            this.indexRecordIterator = startIterator;
        }

        private AscendingIterator(Iterator<IndexBlock> indexBlockIterator){
            this.indexBlockIterator = indexBlockIterator;
        }

        @Override
        public boolean hasNext() {
            if (indexRecordIterator == null || !indexRecordIterator.hasNext()){
                if (!nextRecordIterator()){
                    return false;
                }
            }

            return true;
        }

        @Override
        public IndexRecord next() {
            return indexRecordIterator.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        protected boolean nextRecordIterator(){
            if (!indexBlockIterator.hasNext()){
                return false;
            }

            if (indexBlock != null){
                indexBlock.memory().release();
            }

            indexBlock = indexBlockIterator.next();
            indexRecordIterator = indexBlock.ascendingIterator();

            return true;
        }
    }

    private class DescendingIterator extends AscendingIterator {

        private DescendingIterator(Iterator<IndexBlock> indexBlockIterator, Iterator<IndexRecord> startIterator) {
            super(indexBlockIterator, startIterator);
        }

        private DescendingIterator(Iterator<IndexBlock> indexBlockIterator){
            super(indexBlockIterator);
        }

        @Override
        protected boolean nextRecordIterator(){
            if (!indexBlockIterator.hasNext()){
                return false;
            }

            if (indexBlock != null){
                indexBlock.memory().release();
            }

            indexBlock = indexBlockIterator.next();
            indexRecordIterator = indexBlock.descendingIterator();

            return true;
        }
    }

    private static final int LEAF_BLOCK_OFFSET = 16;

    private final long tableId;
    private final DataFile indexFile;
    private final IndexBlock rootIndexBlock;
    private final NavigableSet<LeafBlock> leafBlocks;
    private final IndexBlock.Cache cache;

    private Index(long tableId, Paths paths, IndexBlock.Cache cache) throws IOException {
        this.tableId = tableId;
        this.indexFile = MutableDataFile.open(paths.indexPath(tableId));
        this.cache = cache;
        long rootIndexBlockOffset = indexFile.readLong(indexFile.size() - Sizes.LONG_SIZE);
        int rootIndexBlockSize = indexFile.readInt(indexFile.size() - Sizes.LONG_SIZE - Sizes.INT_SIZE);
        this.rootIndexBlock = readIndexBlock(rootIndexBlockOffset, rootIndexBlockSize);
        this.leafBlocks = readLeafBlocks();
    }

    public IndexRecord get(Key key) throws IOException {
        IndexRecord currentIndexRecord = rootIndexBlock.get(key);

        while (currentIndexRecord != null && !currentIndexRecord.isLeaf()) {
            IndexBlock currentIndexBlock = readIndexBlock(currentIndexRecord.blockOffset(), currentIndexRecord.blockSize());
            currentIndexRecord = currentIndexBlock.get(key);
        }

        return currentIndexRecord;
    }

    public Iterator<IndexRecord> ascendingIterator() {
        return new AscendingIterator(new IndexBlockIterator(leafBlocks.iterator()));
    }

    public Iterator<IndexRecord> ascendingIterator(Key key) {
        return null;
    }

    public Iterator<IndexRecord> descendingIterator() {
        return new DescendingIterator(new IndexBlockIterator(leafBlocks.descendingIterator()));
    }

    public Iterator<IndexRecord> descendingIterator(Key key) {
        return null;
    }

    public void close() throws IOException {
        indexFile.close();
        cache.clear();
        rootIndexBlock.memory().release();
    }

    private IndexBlock readIndexBlock(long blockOffset, int blockSize) throws IOException {
        IndexBlock indexBlock = cache.get(tableId, blockOffset);

        if (indexBlock == null) {
            Memory indexMemory = Memory.allocate(blockSize);

            try {
                ByteBuffer indexBuffer = indexMemory.directBuffer();
                indexFile.read(indexBuffer, blockOffset);
                indexBuffer.rewind();
                indexBlock = new IndexBlock(new ByteMap(indexMemory));
                cache.put(tableId, blockOffset, indexBlock);
            } catch (IOException e) {
                indexMemory.release();
                throw e;
            }
        }

        return indexBlock;
    }

    private NavigableSet<LeafBlock> readLeafBlocks() throws IOException {
        TreeSet<LeafBlock> leafBlocks = new TreeSet<LeafBlock>();
        long fileOffset = indexFile.size() - LEAF_BLOCK_OFFSET;
        int leafBlockCount = indexFile.readInt(fileOffset);

        ByteBuffer leafBlockBuffer = ByteBuffer.allocate(LeafBlock.SIZE * leafBlockCount);
        indexFile.read(leafBlockBuffer, fileOffset - leafBlockBuffer.capacity());
        leafBlockBuffer.rewind();

        for (int i = 0; i < leafBlockCount; i++){
            long offset = leafBlockBuffer.getLong();
            int size = leafBlockBuffer.getInt();
            leafBlocks.add(new LeafBlock(offset, size));
        }

        return leafBlocks;
    }

    public static Index open(long tableId, Paths paths, IndexBlock.Cache cache) throws IOException {
        return new Index(tableId, paths, cache);
    }
}
