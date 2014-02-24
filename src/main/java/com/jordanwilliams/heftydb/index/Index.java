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

import com.jordanwilliams.heftydb.data.Key;
import com.jordanwilliams.heftydb.io.ChannelDataFile;
import com.jordanwilliams.heftydb.io.DataFile;
import com.jordanwilliams.heftydb.metrics.Metrics;
import com.jordanwilliams.heftydb.offheap.ByteMap;
import com.jordanwilliams.heftydb.offheap.MemoryAllocator;
import com.jordanwilliams.heftydb.offheap.MemoryPointer;
import com.jordanwilliams.heftydb.state.Paths;

import java.io.IOException;
import java.nio.ByteBuffer;

public class Index {

    private static final int ROOT_INDEX_BLOCK_OFFSET = 8;
    private static final int ROOT_INDEX_BLOCK_SIZE_OFFSET = 12;

    private final long tableId;
    private final DataFile indexFile;
    private final long rootBlockOffset;
    private final int rootBlockSize;
    private final IndexBlock.Cache cache;
    private final Metrics metrics;

    private Index(long tableId, DataFile indexFile, IndexBlock.Cache cache, Metrics metrics) throws IOException {
        this.tableId = tableId;
        this.indexFile = indexFile;
        this.cache = cache;
        this.metrics = metrics;
        this.rootBlockOffset = indexFile.readLong(indexFile.size() - ROOT_INDEX_BLOCK_OFFSET);
        this.rootBlockSize = indexFile.readInt(indexFile.size() - ROOT_INDEX_BLOCK_SIZE_OFFSET);
    }

    public IndexRecord get(Key key) throws IOException {
        IndexBlock rootIndexBlock = getIndexBlock(rootBlockOffset, rootBlockSize);
        IndexRecord currentIndexRecord = rootIndexBlock.get(key);
        int searchLevels = 1;

        while (currentIndexRecord != null && !currentIndexRecord.isLeaf()) {
            IndexBlock currentIndexBlock = getIndexBlock(currentIndexRecord.blockOffset(),
                    currentIndexRecord.blockSize());
            currentIndexRecord = currentIndexBlock.get(key);
            currentIndexBlock.memory().release();
            searchLevels++;
        }

        metrics.histogram("index.searchLevels").update(searchLevels);

        rootIndexBlock.memory().release();

        return currentIndexRecord;
    }

    public void close() throws IOException {
        indexFile.close();
        cache.clear();
    }

    private IndexBlock getIndexBlock(long blockOffset, int blockSize) throws IOException {
        IndexBlock indexBlock = cache.get(tableId, blockOffset);
        metrics.hitGauge("index.cacheHitRate").sample(indexBlock != null);

        if (indexBlock == null) {
            indexBlock = readIndexBlock(blockOffset, blockSize);
            cache.put(tableId, blockOffset, indexBlock);
        } else {
            indexBlock.memory().retain();
        }

        return indexBlock;
    }

    private IndexBlock readIndexBlock(long blockOffset, int blockSize) throws IOException {
        MemoryPointer indexPointer = MemoryAllocator.allocate(blockSize);

        try {
            ByteBuffer indexBuffer = indexPointer.directBuffer();
            indexFile.read(indexBuffer, blockOffset);
            indexBuffer.rewind();
            IndexBlock readBlock = new IndexBlock(new ByteMap(indexPointer));
            readBlock.memory().retain();
            return readBlock;
        } catch (IOException e) {
            indexPointer.release();
            throw e;
        }
    }

    public static Index open(long tableId, Paths paths, IndexBlock.Cache cache, Metrics metrics) throws IOException {
        DataFile indexFile = ChannelDataFile.open(paths.indexPath(tableId));
        return new Index(tableId, indexFile, cache, metrics);
    }
}
