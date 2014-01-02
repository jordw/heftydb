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
import com.jordanwilliams.heftydb.offheap.Memory;
import com.jordanwilliams.heftydb.record.Key;
import com.jordanwilliams.heftydb.state.Paths;
import com.jordanwilliams.heftydb.util.Sizes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class Index {

    private final DataFile indexFile;
    private final IndexBlock rootIndexBlock;

    private Index(long tableId, Paths paths) throws IOException {
        this.indexFile = MutableDataFile.open(paths.indexPath(tableId));
        long rootIndexBlockOffset = indexFile.readLong(indexFile.size() - Sizes.LONG_SIZE);
        this.rootIndexBlock = readIndexBlock(rootIndexBlockOffset);
    }

    public List<Long> blockOffsets(Key key) throws IOException {
        List<Long> blockOffsets = new ArrayList<Long>();

        //BFS index blocks
        List<Long> rootBlockOffsets = rootIndexBlock.blockOffsets(key);
        Queue<Long> searchBlockOffsets = new LinkedList<Long>();
        searchBlockOffsets.addAll(rootBlockOffsets);

        while (!searchBlockOffsets.isEmpty()){
            IndexBlock keyIndexBlock = readIndexBlock(searchBlockOffsets.poll());
            List<Long> keyBlockOffsets = keyIndexBlock.blockOffsets(key);

            if (!keyIndexBlock.isLeaf()){
                searchBlockOffsets.addAll(keyBlockOffsets);
            } else {
                blockOffsets.addAll(keyBlockOffsets);
            }

            keyIndexBlock.releaseMemory();
        }

        return blockOffsets;
    }

    public void close() throws IOException {
        indexFile.close();
        rootIndexBlock.releaseMemory();
    }

    private IndexBlock readIndexBlock(long blockOffset) throws IOException {
        int indexBlockSize = indexFile.readInt(blockOffset);
        Memory indexMemory = Memory.allocate(indexBlockSize);
        ByteBuffer indexBuffer = indexMemory.toDirectBuffer();
        indexFile.read(indexBuffer, blockOffset + Sizes.INT_SIZE);
        return new IndexBlock(indexMemory);
    }

    public static Index open(long tableId, Paths paths) throws IOException {
        return new Index(tableId, paths);
    }
}
