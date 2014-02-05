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

package com.jordanwilliams.heftydb.test.helper;

import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.db.Record;
import org.junit.Assert;

import java.util.Iterator;

public class CompareHelper {

    public static void compareKeyValue(Iterator<Tuple> tupleIterator, Iterator<Record> recordIterator) {
        while (tupleIterator.hasNext()) {
            compareKeyValue(tupleIterator.next(), recordIterator.next());
        }

        Assert.assertFalse("Record iterator has next", recordIterator.hasNext());
    }

    public static void compareKeyValue(Tuple tuple, Record record) {
        Assert.assertNotNull("Record is not null", record);
        Assert.assertNotNull("Tuple is not null", record);
        Assert.assertEquals("Keys match", tuple.key().data(), record.key());
        Assert.assertEquals("Values match", tuple.value().data(), record.value());
    }
}
