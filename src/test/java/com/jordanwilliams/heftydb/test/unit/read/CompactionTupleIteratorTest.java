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

package com.jordanwilliams.heftydb.test.unit.read;

import com.jordanwilliams.heftydb.data.Key;
import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.data.Value;
import com.jordanwilliams.heftydb.read.CompactionTupleIterator;
import com.jordanwilliams.heftydb.util.ByteBuffers;
import com.jordanwilliams.heftydb.util.CloseableIterator;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CompactionTupleIteratorTest {

    private static final ByteBuffer KEY_1 = ByteBuffers.fromString("test key 1");
    private static final ByteBuffer KEY_2 = ByteBuffers.fromString("test key 2");
    private static final ByteBuffer KEY_3 = ByteBuffers.fromString("test key 3");

    private final List<Tuple> sourceTuples = new ArrayList<Tuple>();
    private final List<Tuple> filteredTuples = new ArrayList<Tuple>();

    public CompactionTupleIteratorTest() {
        sourceTuples.add(new Tuple(new Key(KEY_3, 0), Value.TOMBSTONE_VALUE));
        sourceTuples.add(new Tuple(new Key(KEY_1, 1), Value.TOMBSTONE_VALUE));
        sourceTuples.add(new Tuple(new Key(KEY_1, 2), Value.TOMBSTONE_VALUE));
        sourceTuples.add(new Tuple(new Key(KEY_1, 3), Value.TOMBSTONE_VALUE));
        sourceTuples.add(new Tuple(new Key(KEY_2, 4), Value.TOMBSTONE_VALUE));
        sourceTuples.add(new Tuple(new Key(KEY_2, 5), Value.TOMBSTONE_VALUE));

        filteredTuples.add(new Tuple(new Key(KEY_3, 0), Value.TOMBSTONE_VALUE));
        filteredTuples.add(new Tuple(new Key(KEY_1, 2), Value.TOMBSTONE_VALUE));
        filteredTuples.add(new Tuple(new Key(KEY_1, 3), Value.TOMBSTONE_VALUE));
        filteredTuples.add(new Tuple(new Key(KEY_2, 4), Value.TOMBSTONE_VALUE));
        filteredTuples.add(new Tuple(new Key(KEY_2, 5), Value.TOMBSTONE_VALUE));
    }

    @Test
    public void basicFilterTest() {
        Iterator<Tuple> filteredIterator = filteredTuples.iterator();
        Iterator<Tuple> versionedIterator = new CompactionTupleIterator(2, new CloseableIterator.Wrapper<Tuple>
                (sourceTuples.iterator()));

        while (versionedIterator.hasNext()) {
            Assert.assertEquals("Records match", filteredIterator.next(), versionedIterator.next());
        }
    }
}
