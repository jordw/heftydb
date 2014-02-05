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

package com.jordanwilliams.heftydb.test.generator;

import com.jordanwilliams.heftydb.data.Key;
import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.data.Value;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class TupleGenerator {

    public interface Function<T> {
        public T apply();
    }

    private final KeyValueGenerator testDataGenerator = new KeyValueGenerator();

    public static ConcurrentNavigableMap<Key, Tuple> toMap(List<Tuple> tuples) {
        ConcurrentNavigableMap<Key, Tuple> recordMap = new ConcurrentSkipListMap<Key, Tuple>();

        for (Tuple tuple : tuples) {
            recordMap.put(tuple.key(), tuple);
        }

        return recordMap;
    }

    public List<Tuple> testRecords(int startingSnapshotId, int recordCount, int keyReuse, Function<Integer> keySize,
                                   Function<Integer> valueSize) {
        int snapshotId = startingSnapshotId;
        List<Tuple> tuples = new ArrayList<Tuple>(recordCount);

        for (int i = 0; i < recordCount; i++) {
            ByteBuffer key = testDataGenerator.testKey(keySize.apply(), keyReuse);
            ByteBuffer value = testDataGenerator.testValue(valueSize.apply());
            tuples.add(new Tuple(new Key(key, snapshotId), new Value(value)));
            snapshotId++;
        }

        Collections.sort(tuples);

        return tuples;
    }


    public List<Tuple> testRecords(int startingSnapshotId, int recordCount, int keyReuse, final int keySize,
                                   final int valueSize) {
        return testRecords(startingSnapshotId, recordCount, keyReuse, new Function<Integer>() {
                    @Override
                    public Integer apply() {
                        return keySize;
                    }
                }, new Function<Integer>() {
                    @Override
                    public Integer apply() {
                        return valueSize;
                    }
                }
        );
    }

    public List<Tuple> testRecords(int recordCount, int keyReuse, int keySize, int valueSize) {
        return testRecords(0, recordCount, keyReuse, keySize, valueSize);
    }

    public List<Tuple> testRecords(int recordCount, int keyReuse) {
        return testRecords(recordCount, keyReuse, 16, 100);
    }

    public static List<Tuple> latest(List<Tuple> tuples, long snapshotId) {
        SortedMap<ByteBuffer, Tuple> latestRecordMap = new TreeMap<ByteBuffer, Tuple>();

        for (Tuple tuple : tuples) {
            Tuple existing = latestRecordMap.get(tuple.key().data());

            if (existing == null || tuple.key().snapshotId() > existing.key().snapshotId()) {
                latestRecordMap.put(tuple.key().data(), tuple);
            }
        }

        return new ArrayList<Tuple>(latestRecordMap.values());
    }
}
