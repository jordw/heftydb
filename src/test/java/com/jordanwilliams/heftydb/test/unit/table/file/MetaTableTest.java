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
import com.jordanwilliams.heftydb.table.file.MetaTable;
import com.jordanwilliams.heftydb.test.base.RecordTest;
import com.jordanwilliams.heftydb.test.generator.ConfigGenerator;
import com.jordanwilliams.heftydb.write.MetaTableWriter;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class MetaTableTest extends RecordTest {

  public MetaTableTest(List<Record> testRecords) {
    super(testRecords);
  }

  @Test
  public void readWriteTest() throws IOException {
    Paths paths = ConfigGenerator.testPaths();
    MetaTableWriter metaWriter = MetaTableWriter.open(1, paths, 2);

    int recordCount = 0;
    long sizeBytes = 0;

    for (Record record : records) {
      metaWriter.write(record);
      recordCount++;
      sizeBytes += record.size();
    }

    metaWriter.finish();

    MetaTable metaTable = MetaTable.open(1, paths);

    Assert.assertEquals("Size matches", sizeBytes, metaTable.sizeBytes());
    Assert.assertEquals("Record count matches", recordCount, metaTable.recordCount());
    Assert.assertEquals("Level matches", 2, metaTable.level());
    Assert.assertEquals("ID matches", 1, metaTable.id());
  }
}
