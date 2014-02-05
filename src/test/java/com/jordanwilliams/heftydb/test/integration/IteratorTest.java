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

package com.jordanwilliams.heftydb.test.integration;

import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.db.HeftyDB;
import com.jordanwilliams.heftydb.db.Record;
import com.jordanwilliams.heftydb.db.Snapshot;
import com.jordanwilliams.heftydb.state.Config;
import com.jordanwilliams.heftydb.test.base.ParameterizedIntegrationTest;
import com.jordanwilliams.heftydb.test.generator.TupleGenerator;
import com.jordanwilliams.heftydb.test.helper.CompareHelper;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class IteratorTest extends ParameterizedIntegrationTest {

    public IteratorTest(List<Tuple> tuples, Config config) throws IOException {
        super(tuples, config);
        writeRecords();
    }

    @Test
    public void ascendingIteratorTest() throws Exception {
        db = HeftyDB.open(config);

        Iterator<Record> dbIterator = db.ascendingIterator(Snapshot.MAX);
        Iterator<Tuple> tupleIterator = TupleGenerator.latest(tuples, Long.MAX_VALUE).iterator();

        CompareHelper.compareKeyValue(tupleIterator, dbIterator);

        db.close();
    }

    @Test
    public void descendingIteratorTest() throws Exception {
        db = HeftyDB.open(config);

        Iterator<Record> dbIterator = db.descendingIterator(Snapshot.MAX);
        List<Tuple> latest = TupleGenerator.latest(tuples, Long.MAX_VALUE);
        Collections.reverse(latest);
        Iterator<Tuple> tupleIterator = latest.iterator();

        CompareHelper.compareKeyValue(tupleIterator, dbIterator);

        db.close();
    }
}
