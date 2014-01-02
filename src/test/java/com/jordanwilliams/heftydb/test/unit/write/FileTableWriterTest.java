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

package com.jordanwilliams.heftydb.test.unit.write;

import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.state.Paths;
import com.jordanwilliams.heftydb.test.base.RecordTest;
import com.jordanwilliams.heftydb.test.generator.ConfigGenerator;
import com.jordanwilliams.heftydb.write.FileTableWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

public class FileTableWriterTest extends RecordTest {

    public FileTableWriterTest(List<Record> testRecords) {
        super(testRecords);
    }

    @Test
    public void readWriteTest() throws IOException {
        Paths paths = ConfigGenerator.defaultDataFiles();
        FileTableWriter fileTableWriter = FileTableWriter.open(1, paths, 100, 512, 1);

        for (Record record : records) {
            fileTableWriter.write(record);
        }

        fileTableWriter.finish();

        Assert.assertTrue("Table file exists", Files.size(paths.tablePath(1)) > 0);
        Assert.assertTrue("Index file exists", Files.size(paths.indexPath(1)) > 0);
        Assert.assertTrue("Filter file exists", Files.size(paths.filterPath(1)) > 0);
    }
}
