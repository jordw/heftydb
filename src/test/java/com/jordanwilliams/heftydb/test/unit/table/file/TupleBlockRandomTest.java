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

import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.table.file.TupleBlock;
import com.jordanwilliams.heftydb.test.base.ParameterizedRecordTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

public class TupleBlockRandomTest extends ParameterizedRecordTest {

    private final TupleBlock tupleBlock;

    public TupleBlockRandomTest(List<Tuple> testTuples) {
        super(testTuples);

        TupleBlock.Builder byteMapBuilder = new TupleBlock.Builder();

        for (Tuple tuple : tuples) {
            byteMapBuilder.addRecord(tuple);
        }

        this.tupleBlock = byteMapBuilder.build();
    }

    @Test
    public void iteratorTest() {
        Iterator<Tuple> recordIterator = tuples.iterator();
        Iterator<Tuple> recordBlockIterator = tupleBlock.ascendingIterator();

        while (recordIterator.hasNext()) {
            Tuple tupleNext = recordIterator.next();
            Tuple blockNext = recordBlockIterator.next();
            Assert.assertEquals("Records match", tupleNext, blockNext);
        }
    }
}
