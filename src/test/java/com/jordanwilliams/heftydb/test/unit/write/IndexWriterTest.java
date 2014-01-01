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

package com.jordanwilliams.heftydb.test.unit.write;

import com.jordanwilliams.heftydb.io.DataFile;
import com.jordanwilliams.heftydb.io.MutableDataFile;
import com.jordanwilliams.heftydb.offheap.Memory;
import com.jordanwilliams.heftydb.record.Key;
import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.state.DataFiles;
import com.jordanwilliams.heftydb.table.file.IndexBlock;
import com.jordanwilliams.heftydb.test.base.RecordTest;
import com.jordanwilliams.heftydb.test.generator.ConfigGenerator;
import com.jordanwilliams.heftydb.util.Sizes;
import com.jordanwilliams.heftydb.write.IndexWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class IndexWriterTest extends RecordTest {

    public IndexWriterTest(List<Record> testRecords) {
        super(testRecords);
    }

    @Test
    public void readWriteTest() throws IOException {
        DataFiles dataFiles = ConfigGenerator.defaultDataFiles();
        IndexWriter indexWriter = IndexWriter.open(1, dataFiles, 512);

        List<Key> keys = new ArrayList<Key>();
        int count = 0;

        for (Record record : records) {
            keys.add(record.key());
            indexWriter.write(new IndexBlock.Record(record.key(), count));
            count++;
        }

        indexWriter.finish();

        DataFile indexFile = MutableDataFile.open(dataFiles.indexPath(1));
        long metaIndexOffset = indexFile.readLong(indexFile.size() - Sizes.LONG_SIZE);
        int metaIndexBlockSize = indexFile.readInt(metaIndexOffset);
        Memory metaIndexMemory = Memory.allocate(metaIndexBlockSize);
        ByteBuffer metaIndexBuffer = metaIndexMemory.toDirectBuffer();
        indexFile.read(metaIndexBuffer, metaIndexOffset + Sizes.INT_SIZE);
        IndexBlock metaIndexBlock = new IndexBlock(metaIndexMemory);

        for (Record record : records) {
            List<Long> blockOffsets = metaIndexBlock.blockOffsets(record.key());

            for (long blockOffset : blockOffsets) {
                int indexBlockSize = indexFile.readInt(blockOffset);
                Memory indexMemory = Memory.allocate(indexBlockSize);
                ByteBuffer indexBuffer = indexMemory.toDirectBuffer();
                indexFile.read(indexBuffer, blockOffset + Sizes.INT_SIZE);
                IndexBlock indexBlock = new IndexBlock(indexMemory);

                Assert.assertTrue(record.key() + " >= " + indexBlock.startKey(), record.key().compareTo(indexBlock.startKey()) >= 0);

                Assert.assertTrue("IndexBlock is a leaf", indexBlock.isLeaf());

                indexBlock.releaseMemory();
            }
        }

        indexFile.close();
    }
}
