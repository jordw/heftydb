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

public class Index {

    private final long tableId;
    private final DataFile indexFile;
    private final IndexBlock rootIndexBlock;
    private final IndexBlock.Cache cache;

    private Index(long tableId, DataFile indexFile, IndexBlock.Cache cache) throws IOException {
        this.tableId = tableId;
        this.indexFile = indexFile;
        this.cache = cache;
        long rootIndexBlockOffset = indexFile.readLong(indexFile.size() - Sizes.LONG_SIZE);
        int rootIndexBlockSize = indexFile.readInt(indexFile.size() - Sizes.LONG_SIZE - Sizes.INT_SIZE);
        this.rootIndexBlock = readIndexBlock(rootIndexBlockOffset, rootIndexBlockSize);
    }

    public IndexRecord get(Key key) throws IOException {
        IndexRecord currentIndexRecord = rootIndexBlock.get(key);

        while (currentIndexRecord != null && !currentIndexRecord.isLeaf()) {
            IndexBlock currentIndexBlock = getIndexBlock(currentIndexRecord.blockOffset(),
                    currentIndexRecord.blockSize());
            currentIndexRecord = currentIndexBlock.get(key);
        }

        return currentIndexRecord;
    }

    public void close() throws IOException {
        indexFile.close();
        cache.clear();
        rootIndexBlock.memory().release();
    }

    private IndexBlock getIndexBlock(long blockOffset, int blockSize) throws IOException {
        IndexBlock indexBlock = cache.get(tableId, blockOffset);

        if (indexBlock == null) {
            indexBlock = readIndexBlock(blockOffset, blockSize);
            cache.put(tableId, blockOffset, indexBlock);
        }

        return indexBlock;
    }

    private IndexBlock readIndexBlock(long blockOffset, int blockSize) throws IOException {
        Memory indexMemory = Memory.allocate(blockSize);

        try {
            ByteBuffer indexBuffer = indexMemory.directBuffer();
            indexFile.read(indexBuffer, blockOffset);
            indexBuffer.rewind();
            return new IndexBlock(new ByteMap(indexMemory));
        } catch (IOException e) {
            indexMemory.release();
            throw e;
        }

    }

    public static Index open(long tableId, Paths paths, IndexBlock.Cache cache) throws IOException {
        DataFile indexFile = MutableDataFile.open(paths.indexPath(tableId));
        return new Index(tableId, indexFile, cache);
    }
}
