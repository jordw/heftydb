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

package com.jordanwilliams.heftydb.test.unit.table.memory;

import com.jordanwilliams.heftydb.record.Key;
import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.table.Table;
import com.jordanwilliams.heftydb.table.memory.MemoryTable;
import com.jordanwilliams.heftydb.test.base.RecordTest;
import com.jordanwilliams.heftydb.util.ByteBuffers;

import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class MemoryTableTest extends RecordTest {

  private static final Key MID_KEY = new Key(ByteBuffers.fromString("JKLMNOP"));

  public MemoryTableTest(List<Record> testRecords) {
    super(testRecords);
  }

  @Test
  public void readWriteTest() {
    MemoryTable memoryTable = new MemoryTable(1);

    for (Record record : records) {
      memoryTable.put(record);
    }

    for (Record record : recordGenerator.latestRecords(records, Long.MAX_VALUE)) {
      Assert.assertEquals("Records match", record, memoryTable.get(record.key(), Long.MAX_VALUE));
    }
  }

  @Test
  public void mightContainTest() {
    MemoryTable memoryTable = new MemoryTable(1);

    for (Record record : records) {
      memoryTable.put(record);
    }

    for (Record record : recordGenerator.latestRecords(records, Long.MAX_VALUE)) {
      Assert.assertTrue("Records are contained", memoryTable.mightContain(record.key()));
    }
  }

  @Test
  public void ascendingIteratorTest() {
    MemoryTable memoryTable = new MemoryTable(1);

    for (Record record : records) {
      memoryTable.put(record);
    }

    Iterator<Record>
        ascendingIterator =
        memoryTable.iterator(Table.IterationDirection.ASCENDING, Long.MAX_VALUE);
    ListIterator<Record>
        latestRecordIterator =
        recordGenerator.latestRecords(records, Long.MAX_VALUE).listIterator();

    while (latestRecordIterator.hasNext()) {
      Assert.assertEquals("Records match", latestRecordIterator.next(), ascendingIterator.next());
    }
  }

  @Test
  public void ascendingRangeIteratorTest() {
    List<Record> records = recordGenerator.testRecords(100, 20);
    MemoryTable memoryTable = new MemoryTable(1);

    for (Record record : records) {
      memoryTable.put(record);
    }

    Iterator<Record>
        ascendingRangeIterator =
        memoryTable.iteratorFrom(MID_KEY, Table.IterationDirection.ASCENDING, Long.MAX_VALUE);
    ListIterator<Record>
        latestRecordIterator =
        recordGenerator.latestRecords(records, Long.MAX_VALUE).listIterator();

    while (latestRecordIterator.hasNext()) {
      Record next = latestRecordIterator.next();

      if (next.key().compareTo(MID_KEY) >= 0) {
        Assert.assertEquals("Records match", next, ascendingRangeIterator.next());
      }
    }
  }

  @Test
  public void descendingIteratorTest() {
    MemoryTable memoryTable = new MemoryTable(1);

    for (Record record : records) {
      memoryTable.put(record);
    }

    Iterator<Record>
        descendingIterator =
        memoryTable.iterator(Table.IterationDirection.DESCENDING, Long.MAX_VALUE);
    ListIterator<Record>
        latestRecordIterator =
        recordGenerator.latestRecords(records, Long.MAX_VALUE).listIterator();

    while (latestRecordIterator.hasPrevious()) {
      Assert.assertEquals("Records match", latestRecordIterator.previous(),
                          descendingIterator.next());
    }
  }

  @Test
  public void descendingRangeIteratorTest() {
    MemoryTable memoryTable = new MemoryTable(1);

    for (Record record : records) {
      memoryTable.put(record);
    }

    Iterator<Record>
        descendingRangeIterator =
        memoryTable.iteratorFrom(MID_KEY, Table.IterationDirection.ASCENDING, Long.MAX_VALUE);
    ListIterator<Record>
        latestRecordIterator =
        recordGenerator.latestRecords(records, Long.MAX_VALUE).listIterator();

    while (latestRecordIterator.hasPrevious()) {
      Record next = latestRecordIterator.previous();

      if (next.key().compareTo(MID_KEY) <= 0) {
        Assert.assertEquals("Records match", next, descendingRangeIterator.next());
      }
    }
  }
}
