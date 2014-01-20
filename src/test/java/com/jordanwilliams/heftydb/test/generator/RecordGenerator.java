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

import com.jordanwilliams.heftydb.record.Key;
import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.record.Value;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class RecordGenerator {

    private final KeyValueGenerator testDataGenerator = new KeyValueGenerator();

    public static ConcurrentNavigableMap<Key, Record> toMap(List<Record> records) {
        ConcurrentNavigableMap<Key, Record> recordMap = new ConcurrentSkipListMap<Key, Record>();

        for (Record record : records) {
            recordMap.put(record.key(), record);
        }

        return recordMap;
    }

    public List<Record> testRecords(int startingSnapshotId, int recordCount, int keyReuse, int keySize, int valueSize) {
        int snapshotId = startingSnapshotId;
        List<Record> records = new ArrayList<Record>(recordCount);

        for (int i = 0; i < recordCount; i++) {
            ByteBuffer key = testDataGenerator.testKey(keySize, keyReuse);
            ByteBuffer value = testDataGenerator.testValue(valueSize);
            records.add(new Record(new Key(key, snapshotId), new Value(value)));
            snapshotId++;
        }

        Collections.sort(records);

        return records;
    }

    public List<Record> testRecords(int recordCount, int keyReuse, int keySize, int valueSize) {
        return testRecords(0, recordCount, keyReuse, keySize, valueSize);
    }

    public List<Record> testRecords(int recordCount, int keyReuse) {
        return testRecords(recordCount, keyReuse, 16, 100);
    }

    public Iterator<Record> testRecordIterator(int recordCount, int keyReuse, int keySize, int valueSize) {
        return testRecords(recordCount, keyReuse, keySize, valueSize).iterator();
    }

    public List<Record> latestRecords(List<Record> records, long snapshotId) {
        SortedMap<ByteBuffer, Record> latestRecordMap = new TreeMap<ByteBuffer, Record>();

        for (Record record : records) {
            Record existing = latestRecordMap.get(record.key().data());

            if (existing == null || record.key().snapshotId() > existing.key().snapshotId()) {
                latestRecordMap.put(record.key().data(), record);
            }
        }

        return new ArrayList<Record>(latestRecordMap.values());
    }
}
