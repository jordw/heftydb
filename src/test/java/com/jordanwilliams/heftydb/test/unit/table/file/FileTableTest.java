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

package com.jordanwilliams.heftydb.test.unit.table.file;

import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.state.Paths;
import com.jordanwilliams.heftydb.table.file.FileTable;
import com.jordanwilliams.heftydb.table.file.IndexBlock;
import com.jordanwilliams.heftydb.test.base.RecordTest;
import com.jordanwilliams.heftydb.test.generator.ConfigGenerator;
import com.jordanwilliams.heftydb.write.FileTableWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class FileTableTest extends RecordTest {

    public FileTableTest(List<Record> testRecords) {
        super(testRecords);
    }

    @Test
    public void readWriteTest() throws IOException {
        Paths paths = ConfigGenerator.testPaths();
        FileTableWriter tableWriter = FileTableWriter.open(1, paths, 100, 512, 1);

        for (Record record : records){
            tableWriter.write(record);
        }

        tableWriter.finish();
        FileTable fileTable = FileTable.open(1, paths, new IndexBlock.Cache());

        for (Record record : records){
            Record read = fileTable.get(record.key(), record.snapshotId());
            Assert.assertEquals("Records match", record, read);
        }
    }
}
