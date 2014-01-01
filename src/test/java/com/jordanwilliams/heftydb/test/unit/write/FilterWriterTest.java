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
import com.jordanwilliams.heftydb.offheap.BloomFilter;
import com.jordanwilliams.heftydb.offheap.Memory;
import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.state.DataFiles;
import com.jordanwilliams.heftydb.test.base.RecordTest;
import com.jordanwilliams.heftydb.test.generator.ConfigGenerator;
import com.jordanwilliams.heftydb.write.FilterWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class FilterWriterTest extends RecordTest {

    public FilterWriterTest(List<Record> testRecords) {
        super(testRecords);
    }

    @Test
    public void readWriteTest() throws IOException {
        DataFiles dataFiles = ConfigGenerator.defaultDataFiles();
        FilterWriter filterWriter = FilterWriter.open(1, dataFiles, records.size());

        for (Record record : records) {
            filterWriter.write(record);
        }

        filterWriter.finish();

        DataFile filterFile = MutableDataFile.open(dataFiles.filterPath(1));
        Memory filterMemory = Memory.allocate(filterFile.size());
        ByteBuffer filterBuffer = filterMemory.toDirectBuffer();
        filterFile.read(filterBuffer, 0);
        BloomFilter testFilter = new BloomFilter(filterMemory);

        for (Record record : records) {
            Assert.assertTrue("Record is in the filter", testFilter.mightContain(record.key().data().array()));
        }

        filterMemory.release();
    }
}
