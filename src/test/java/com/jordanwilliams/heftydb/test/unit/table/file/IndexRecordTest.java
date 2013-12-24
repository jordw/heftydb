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

package com.jordanwilliams.heftydb.test.unit.table.file;

import com.jordanwilliams.heftydb.record.Key;
import com.jordanwilliams.heftydb.table.file.IndexRecord;
import com.jordanwilliams.heftydb.util.ByteBuffers;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class IndexRecordTest {

    private static final Key TEST_START_KEY = new Key(ByteBuffers.fromString("I am a test key"));

    @Test
    public void testSerialization() {
        IndexRecord indexRecord = new IndexRecord(TEST_START_KEY, 1);
        ByteBuffer serialized = IndexRecord.SERIALIZER.serialize(indexRecord);
        IndexRecord deserializedRecord = IndexRecord.SERIALIZER.deserialize(serialized);

        Assert.assertEquals("Deserialized index records match", indexRecord, deserializedRecord);
    }
}
