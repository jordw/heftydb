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

package com.jordanwilliams.heftydb.test.unit.offheap;

import com.jordanwilliams.heftydb.offheap.ByteMap;
import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.test.generator.RecordGenerator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

public class ByteMapTest {

    private static final ByteMap BYTE_MAP;
    private static final List<Record> TEST_RECORDS;

    static {
        ByteMap.Builder byteMapBuilder = new ByteMap.Builder();
        RecordGenerator recordGenerator = new RecordGenerator();
        TEST_RECORDS = recordGenerator.testRecords(100, 0);

        for (Record record : TEST_RECORDS){
            byteMapBuilder.add(record.key(), record.value());
        }

        BYTE_MAP = byteMapBuilder.build();
    }

    @Test
    public void getTest(){
        for (int i = 0; i < TEST_RECORDS.size(); i++){
            ByteMap.Entry entry = BYTE_MAP.get(i);
            Record record = TEST_RECORDS.get(i);
            Assert.assertEquals("Keys match", record.key(), entry.key());
            Assert.assertEquals("Values match", record.value(), entry.value());
        }
    }

    @Test
    public void floorIndexMatchTest(){
        for (int i = 0; i < TEST_RECORDS.size(); i++){
            Record record = TEST_RECORDS.get(i);
            int floorIndex = BYTE_MAP.floorIndex(record.key());
            ByteMap.Entry entry = BYTE_MAP.get(floorIndex);
            Assert.assertEquals("Keys match", record.key(), entry.key());
            Assert.assertEquals("Values match", record.value(), entry.value());
        }
    }

    @Test
    public void iteratorTest(){
        Iterator<Record> testRecords = TEST_RECORDS.iterator();

        for (ByteMap.Entry entry : BYTE_MAP){
            Record record = testRecords.next();
            Assert.assertEquals("Keys match", record.key(), entry.key());
            Assert.assertEquals("Values match", record.value(), entry.value());
        }
    }
}
