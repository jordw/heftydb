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
import com.jordanwilliams.heftydb.table.file.Index;
import com.jordanwilliams.heftydb.table.file.IndexBlock;

import java.io.IOException;
import java.nio.ByteBuffer;

public class IndexWriter {

    private static final int MAX_INDEX_BLOCK_SIZE_BYTES = 65536;

    private final long tableId;
    private final DataFiles dataFiles;
    private final Index.Builder indexBuilder;
    private final DataFile indexFile;

    private IndexWriter(long tableId, DataFiles dataFiles) throws IOException {
        this.tableId = tableId;
        this.dataFiles = dataFiles;
        this.indexBuilder = new Index.Builder();
        this.indexFile = MutableDataFile.open(dataFiles.indexPath(tableId));
    }

    public void write(IndexBlock.Record indexRecord) throws IOException {
        if (indexBuilder.currentBlockSizeBytes() >= MAX_INDEX_BLOCK_SIZE_BYTES){
            IndexBlock indexBlock = indexBuilder.newIndexBlock();
            ByteBuffer indexBlockBuffer = indexBlock.memory().toDirectBuffer();
            indexFile.append(indexBlockBuffer);
        }

        indexBuilder.addRecord(indexRecord);
    }

    public void finish() throws IOException {
        IndexBlock metaIndexBlock = indexBuilder.finish();
        ByteBuffer metaIndexBlockBuffer = metaIndexBlock.memory().toDirectBuffer();
        indexFile.append(metaIndexBlockBuffer);
        indexFile.close();
    }

    public static IndexWriter open(long tableId, DataFiles dataFiles) throws IOException {
        return new IndexWriter(tableId, dataFiles);
    }
}
