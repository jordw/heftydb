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

package com.jordanwilliams.heftydb.test.unit.data;

import com.jordanwilliams.heftydb.data.Key;
import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.data.Value;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class TupleTest {

    private static final ByteBuffer TEST_KEY = ByteBuffer.wrap("I am a test key".getBytes());
    private static final ByteBuffer TEST_VALUE = ByteBuffer.wrap("I am a test value".getBytes());

    @Test
    public void testSerialization() {
        Tuple tuple = new Tuple(new Key(TEST_KEY, 1), new Value(TEST_VALUE));
        ByteBuffer serialized = ByteBuffer.allocate(Tuple.SERIALIZER.size(tuple));
        Tuple.SERIALIZER.serialize(tuple, serialized);
        Tuple deserializedTuple = Tuple.SERIALIZER.deserialize(serialized);

        Assert.assertEquals("Deserialized tuples match", tuple, deserializedTuple);
    }

    @Test
    public void testTombstoneSerialization() {
        Tuple tuple = new Tuple(new Key(TEST_KEY, 1), Value.TOMBSTONE_VALUE);
        ByteBuffer serialized = ByteBuffer.allocate(Tuple.SERIALIZER.size(tuple));
        Tuple.SERIALIZER.serialize(tuple, serialized);
        Tuple deserializedTuple = Tuple.SERIALIZER.deserialize(serialized);

        Assert.assertEquals("Deserialized tombstone tuples match", tuple, deserializedTuple);
        Assert.assertTrue("Tuple is a tombstone tuple", deserializedTuple.value().isEmpty());
    }
}