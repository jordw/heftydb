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
import java.util.List;

public class IndexWriter {

    private static final int MAX_INDEX_BLOCK_SIZE_BYTES = 65536;

    private final long tableId;
    private final DataFiles dataFiles;
    private final DataFile indexFile;
    private final List<IndexBlock.Record> metaIndexRecords = new ArrayList<IndexBlock.Record>();
    private final int maxIndexBlockSizeBytes;

    private IndexBlock.Builder indexBlockBuilder;

    private IndexWriter(long tableId, DataFiles dataFiles, int maxIndexBlockSizeBytes) throws IOException {
        this.tableId = tableId;
        this.dataFiles = dataFiles;
        this.indexBlockBuilder = new IndexBlock.Builder();
        this.indexFile = MutableDataFile.open(dataFiles.indexPath(tableId));
        this.maxIndexBlockSizeBytes = maxIndexBlockSizeBytes;
    }

    public void write(IndexBlock.Record indexRecord) throws IOException {
        if (indexBlockBuilder.sizeBytes() >= maxIndexBlockSizeBytes) {
            writeIndexBlock();
        }

        indexBlockBuilder.addRecord(indexRecord);
    }

    public void finish() throws IOException {
        if (indexBlockBuilder.sizeBytes() > 0){
            writeIndexBlock();
        }

        IndexBlock metaIndexBlock = buildMetaIndexBlock();
        ByteBuffer metaIndexBlockBuffer = metaIndexBlock.memory().toDirectBuffer();

        long indexSizeOffset = indexFile.appendInt(metaIndexBlockBuffer.capacity());
        indexFile.append(metaIndexBlockBuffer);
        indexFile.appendLong(indexSizeOffset);
        indexFile.close();
        metaIndexBlock.releaseMemory();
    }

    private void writeIndexBlock() throws IOException {
        IndexBlock indexBlock = indexBlockBuilder.build();
        ByteBuffer indexBlockBuffer = indexBlock.memory().toDirectBuffer();
        long indexBlockOffset = indexFile.appendInt(indexBlockBuffer.capacity());
        indexFile.append(indexBlockBuffer);

        indexBlockBuilder = new IndexBlock.Builder();
        metaIndexRecords.add(new IndexBlock.Record(indexBlock.startKey(), indexBlockOffset));
        indexBlock.releaseMemory();
    }

    private IndexBlock buildMetaIndexBlock(){
        IndexBlock.Builder metaIndexBuilder = new IndexBlock.Builder();

        for (IndexBlock.Record metaIndexRecord : metaIndexRecords){
            metaIndexBuilder.addRecord(metaIndexRecord);
        }

        return metaIndexBuilder.build();
    }

    public static IndexWriter open(long tableId, DataFiles dataFiles, int maxIndexSizeBytes) throws IOException {
        return new IndexWriter(tableId, dataFiles, maxIndexSizeBytes);
    }

    public static IndexWriter open(long tableId, DataFiles dataFiles) throws IOException {
        return new IndexWriter(tableId, dataFiles, MAX_INDEX_BLOCK_SIZE_BYTES);
    }
}
