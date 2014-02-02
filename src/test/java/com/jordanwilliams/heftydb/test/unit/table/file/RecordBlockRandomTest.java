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
import com.jordanwilliams.heftydb.table.file.RecordBlock;
import com.jordanwilliams.heftydb.test.base.ParameterizedRecordTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

public class RecordBlockRandomTest extends ParameterizedRecordTest {

    private final RecordBlock recordBlock;

    public RecordBlockRandomTest(List<Record> testRecords) {
        super(testRecords);

        RecordBlock.Builder byteMapBuilder = new RecordBlock.Builder();

        for (Record record : records){
            byteMapBuilder.addRecord(record);
        }

        this.recordBlock = byteMapBuilder.build();
    }

    @Test
    public void iteratorTest(){
        Iterator<Record> recordIterator = records.iterator();
        Iterator<Record> recordBlockIterator = recordBlock.ascendingIterator();

        while (recordIterator.hasNext()){
            Record recordNext = recordIterator.next();
            Record blockNext = recordBlockIterator.next();
            Assert.assertEquals("Records match", recordNext, blockNext);
        }
    }
}
