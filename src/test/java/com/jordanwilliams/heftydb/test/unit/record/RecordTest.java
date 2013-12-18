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

package com.jordanwilliams.heftydb.test.unit.record;

import com.jordanwilliams.heftydb.record.Record;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class RecordTest {

    private static final ByteBuffer TEST_KEY = ByteBuffer.wrap("I am a test key".getBytes());
    private static final ByteBuffer TEST_VALUE = ByteBuffer.wrap("I am a test value".getBytes());

    @Test
    public void testSerialization() {
        Record record = new Record(TEST_KEY, TEST_VALUE, 1);
        ByteBuffer serialized = Record.SERIALIZER.serialize(record);
        Record deserializedRecord = Record.SERIALIZER.deserialize(serialized);

        Assert.assertEquals("Deserialized records match", record, deserializedRecord);
    }

    @Test
    public void testTombstoneSerialization() {
        Record record = new Record(TEST_KEY, null, 1);
        ByteBuffer serialized = Record.SERIALIZER.serialize(record);
        Record deserializedRecord = Record.SERIALIZER.deserialize(serialized);

        Assert.assertEquals("Deserialized tombstone records match", record, deserializedRecord);
        Assert.assertTrue("DBRecord is a tombstone record", deserializedRecord.tombstone());
    }
}
