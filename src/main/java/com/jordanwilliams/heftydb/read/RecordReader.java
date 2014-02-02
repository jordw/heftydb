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

package com.jordanwilliams.heftydb.read;

import com.jordanwilliams.heftydb.record.Key;
import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.state.State;
import com.jordanwilliams.heftydb.table.Table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RecordReader implements Iterable<Record> {

    private final State state;

    public RecordReader(State state) {
        this.state = state;
    }

    public Record get(Key key) {
        Record closestRecord = null;

        for (Table table : state.tables()){
            if (table.mightContain(key)){
                Record tableRecord = table.get(key);

                if (tableRecord != null){
                    if (closestRecord == null || tableRecord.key().snapshotId() > closestRecord.key().snapshotId()){
                        closestRecord = tableRecord;
                    }
                }
            }
        }

        return closestRecord;
    }

    public Iterator<Record> ascendingIterator(long snapshotId) {
        List<Iterator<Record>> tableIterators = new ArrayList<Iterator<Record>>();

        for (Table table : state.tables()){
            tableIterators.add(table.ascendingIterator(snapshotId));
        }

        return new MergingIterator<Record>(tableIterators);
    }

    public Iterator<Record> descendingIterator(long snapshotId) {
        List<Iterator<Record>> tableIterators = new ArrayList<Iterator<Record>>();

        for (Table table : state.tables()){
            tableIterators.add(table.descendingIterator(snapshotId));
        }

        return new MergingIterator<Record>(tableIterators);
    }

    public Iterator<Record> ascendingIterator(Key key, long snapshotId) {
        List<Iterator<Record>> tableIterators = new ArrayList<Iterator<Record>>();

        for (Table table : state.tables()){
            tableIterators.add(table.ascendingIterator(key, snapshotId));
        }

        return new MergingIterator<Record>(tableIterators);
    }

    public Iterator<Record> descendingIterator(Key key, long snapshotId) {
        List<Iterator<Record>> tableIterators = new ArrayList<Iterator<Record>>();

        for (Table table : state.tables()){
            tableIterators.add(table.descendingIterator(key, snapshotId));
        }

        return new MergingIterator<Record>(tableIterators);
    }

    public void close() throws IOException {
        for (Table table : state.tables()){
            table.close();
        }
    }

    @Override
    public Iterator<Record> iterator() {
        return ascendingIterator(Long.MAX_VALUE);
    }
}
