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

import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.offheap.SortedByteMap;
import com.jordanwilliams.heftydb.test.base.ParameterizedTupleTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

public class SortedByteMapTest extends ParameterizedTupleTest {

    private final SortedByteMap sortedByteMap;

    public SortedByteMapTest(List<Tuple> testTuples) {
        super(testTuples);

        SortedByteMap.Builder byteMapBuilder = new SortedByteMap.Builder();

        for (Tuple tuple : tuples) {
            byteMapBuilder.add(tuple.key(), tuple.value());
        }

        this.sortedByteMap = byteMapBuilder.build();
    }

    @Test
    public void getTest() {
        for (int i = 0; i < tuples.size(); i++) {
            SortedByteMap.Entry entry = sortedByteMap.get(i);
            Tuple tuple = tuples.get(i);
            Assert.assertEquals("Keys match", tuple.key(), entry.key());
            Assert.assertEquals("Values match", tuple.value(), entry.value());
        }
    }

    @Test
    public void floorIndexMatchTest() {
        for (int i = 0; i < tuples.size(); i++) {
            Tuple tuple = tuples.get(i);
            int floorIndex = sortedByteMap.floorIndex(tuple.key());
            SortedByteMap.Entry entry = sortedByteMap.get(floorIndex);
            Assert.assertEquals("Keys match", tuple.key(), entry.key());
            Assert.assertEquals("Values match", tuple.value(), entry.value());
        }
    }


    @Test
    public void iteratorTest() {
        Iterator<Tuple> recordIterator = tuples.iterator();
        Iterator<SortedByteMap.Entry> byteMapIterator = sortedByteMap.iterator();

        while (recordIterator.hasNext()) {
            Tuple tupleNext = recordIterator.next();
            SortedByteMap.Entry byteMapNext = byteMapIterator.next();

            Assert.assertEquals("Records match", tupleNext, new Tuple(byteMapNext.key(), byteMapNext.value()));
        }
    }
}
