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

package com.jordanwilliams.heftydb.test.unit.table.memory;

import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.table.memory.MemoryTable;
import com.jordanwilliams.heftydb.test.base.RecordTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class MemoryTableTest extends RecordTest {

    @Test
    public void readWriteTest(){
        List<Record> records = recordGenerator.testRecords(100, 20);
        MemoryTable memoryTable = new MemoryTable(1);

        for (Record record : records){
            memoryTable.put(record);
        }

        for (Record record : recordGenerator.latestRecords(records, Long.MAX_VALUE)){
            Assert.assertEquals("Records match", record, memoryTable.get(record.key(), Long.MAX_VALUE));
        }
    }
}
