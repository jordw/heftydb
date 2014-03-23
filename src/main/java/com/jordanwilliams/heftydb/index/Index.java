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

package com.jordanwilliams.heftydb.index;

import com.codahale.metrics.Histogram;
import com.jordanwilliams.heftydb.data.Key;
import com.jordanwilliams.heftydb.io.ImmutableChannelFile;
import com.jordanwilliams.heftydb.io.ImmutableFile;
import com.jordanwilliams.heftydb.metrics.CacheHitGauge;
import com.jordanwilliams.heftydb.metrics.Metrics;
import com.jordanwilliams.heftydb.offheap.SortedByteMap;
import com.jordanwilliams.heftydb.offheap.MemoryAllocator;
import com.jordanwilliams.heftydb.offheap.MemoryPointer;
import com.jordanwilliams.heftydb.state.Paths;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Represents a read-only view of a B+tree database index file.
 */
public class Index {

    private static final int ROOT_INDEX_BLOCK_OFFSET = 8;
    private static final int ROOT_INDEX_BLOCK_SIZE_OFFSET = 12;

    private final long tableId;
    private final ImmutableFile indexFile;
    private final IndexBlock.Cache cache;
    private final Metrics metrics;
    private final IndexBlock rootIndexBlock;

    private final Histogram indexSearchLevels;
    private final CacheHitGauge indexCacheHitRate;

    private Index(long tableId, ImmutableFile indexFile, IndexBlock.Cache cache, Metrics metrics) throws IOException {
        this.tableId = tableId;
        this.indexFile = indexFile;
        this.cache = cache;
        this.metrics = metrics;
        long rootBlockOffset = indexFile.readLong(indexFile.size() - ROOT_INDEX_BLOCK_OFFSET);
        int rootBlockSize = indexFile.readInt(indexFile.size() - ROOT_INDEX_BLOCK_SIZE_OFFSET);
        this.rootIndexBlock = readIndexBlock(rootBlockOffset, rootBlockSize);

        this.indexSearchLevels = metrics.histogram("index.searchLevels");
        this.indexCacheHitRate = metrics.hitGauge("index.cacheHitRate");
    }

    public IndexRecord get(Key key) throws IOException {
        IndexRecord currentIndexRecord = rootIndexBlock.get(key);
        int searchLevels = 1;

        while (currentIndexRecord != null && !currentIndexRecord.isLeaf()) {
            IndexBlock currentIndexBlock = getIndexBlock(currentIndexRecord.blockOffset(),
                    currentIndexRecord.blockSize());
            currentIndexRecord = currentIndexBlock.get(key);
            currentIndexBlock.memory().release();
            searchLevels++;
        }

        indexSearchLevels.update(searchLevels);

        return currentIndexRecord;
    }

    public void close() throws IOException {
        rootIndexBlock.memory().release();
        indexFile.close();
        cache.clear();
    }

    private IndexBlock getIndexBlock(long blockOffset, int blockSize) throws IOException {
        IndexBlock indexBlock = cache.get(tableId, blockOffset);
        indexCacheHitRate.sample(indexBlock != null);

        if (indexBlock == null) {
            indexBlock = readIndexBlock(blockOffset, blockSize);
            cache.put(tableId, blockOffset, indexBlock);
        }

        return indexBlock;
    }

    private IndexBlock readIndexBlock(long blockOffset, int blockSize) throws IOException {
        MemoryPointer indexPointer = MemoryAllocator.allocate(blockSize);

        try {
            ByteBuffer indexBuffer = indexPointer.directBuffer();
            indexFile.read(indexBuffer, blockOffset);
            indexBuffer.rewind();
            return new IndexBlock(new SortedByteMap(indexPointer));
        } catch (IOException e) {
            indexPointer.release();
            throw e;
        }
    }

    public static Index open(long tableId, Paths paths, IndexBlock.Cache cache, Metrics metrics) throws IOException {
        ImmutableFile indexFile = ImmutableChannelFile.open(paths.indexPath(tableId));
        return new Index(tableId, indexFile, cache, metrics);
    }
}
