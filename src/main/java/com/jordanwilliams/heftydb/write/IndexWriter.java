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

package com.jordanwilliams.heftydb.write;

import com.jordanwilliams.heftydb.io.DataFile;
import com.jordanwilliams.heftydb.io.MutableDataFile;
import com.jordanwilliams.heftydb.state.DataFiles;
import com.jordanwilliams.heftydb.table.file.IndexBlock;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class IndexWriter {

    private static final int MAX_INDEX_BLOCK_SIZE_BYTES = 65536;

    private final DataFile indexFile;
    private final int maxIndexBlockSizeBytes;
    private final List<IndexBlock.Builder> indexBlockBuilders = new ArrayList<IndexBlock.Builder>();

    private IndexWriter(long tableId, DataFiles dataFiles, int maxIndexBlockSizeBytes) throws IOException {
        this.indexFile = MutableDataFile.open(dataFiles.indexPath(tableId));
        this.maxIndexBlockSizeBytes = maxIndexBlockSizeBytes;
        indexBlockBuilders.add(new IndexBlock.Builder(true));
    }

    public void write(IndexBlock.Record indexRecord) throws IOException {
        Queue<IndexBlock.Record> pendingIndexRecord = new LinkedList<IndexBlock.Record>();
        pendingIndexRecord.add(indexRecord);

        for (int i = 0; i < indexBlockBuilders.size(); i++){
            if (pendingIndexRecord.isEmpty()){
                return;
            }

            IndexBlock.Builder levelBuilder = indexBlockBuilders.get(i);

            if (levelBuilder.sizeBytes() >= maxIndexBlockSizeBytes){
                IndexBlock.Record metaRecord = writeIndexBlock(levelBuilder.build());

                IndexBlock.Builder newLevelBuilder = new IndexBlock.Builder(i == 0);
                newLevelBuilder.addRecord(pendingIndexRecord.poll());
                indexBlockBuilders.set(i, newLevelBuilder);

                pendingIndexRecord.add(metaRecord);
            } else {
                levelBuilder.addRecord(pendingIndexRecord.poll());
            }
        }

        if (!pendingIndexRecord.isEmpty()){
            IndexBlock.Builder newLevelBuilder = new IndexBlock.Builder(false);
            newLevelBuilder.addRecord(pendingIndexRecord.poll());
            indexBlockBuilders.add(newLevelBuilder);
        }
    }

    public void finish() throws IOException {
        Queue<IndexBlock.Record> pendingIndexRecord = new LinkedList<IndexBlock.Record>();

        for (int i = 0; i < indexBlockBuilders.size(); i++){
            IndexBlock.Builder levelBuilder = indexBlockBuilders.get(i);

            if (!pendingIndexRecord.isEmpty()){
                levelBuilder.addRecord(pendingIndexRecord.poll());
            }

            IndexBlock.Record nextLevelRecord = writeIndexBlock(levelBuilder.build());
            pendingIndexRecord.add(nextLevelRecord);
        }

        long rootIndexBlockOffset = pendingIndexRecord.poll().offset();
        indexFile.appendLong(rootIndexBlockOffset);
        indexFile.close();
    }

    private IndexBlock.Record writeIndexBlock(IndexBlock indexBlock) throws IOException {
        ByteBuffer indexBlockBuffer = indexBlock.memory().toDirectBuffer();
        long indexBlockOffset = indexFile.appendInt(indexBlockBuffer.capacity());
        indexFile.append(indexBlockBuffer);
        IndexBlock.Record metaIndexRecord = new IndexBlock.Record(indexBlock.startKey(), indexBlockOffset);
        indexBlock.releaseMemory();
        return metaIndexRecord;
    }

    public static IndexWriter open(long tableId, DataFiles dataFiles, int maxIndexSizeBytes) throws IOException {
        return new IndexWriter(tableId, dataFiles, maxIndexSizeBytes);
    }

    public static IndexWriter open(long tableId, DataFiles dataFiles) throws IOException {
        return new IndexWriter(tableId, dataFiles, MAX_INDEX_BLOCK_SIZE_BYTES);
    }
}
