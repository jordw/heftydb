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

package com.jordanwilliams.heftydb.read;

import com.jordanwilliams.heftydb.record.Record;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.SortedSet;
import java.util.TreeSet;

public class VersionedRecordIterator implements Iterator<Record> {

  private final Iterator<Record> recordIterator;
  private final Queue<Record> nextRecord = new LinkedList<Record>();
  private final long maxSnapshotId;
  private final SortedSet<Record> currentKeyRecords = new TreeSet<Record>();

  public VersionedRecordIterator(long maxSnapshotId, Iterator<Record> recordIterator) {
    this.maxSnapshotId = maxSnapshotId;
    this.recordIterator = recordIterator;
  }

  @Override
  public boolean hasNext() {
    if (!nextRecord.isEmpty()) {
      return true;
    }

    Record record = fetchNextRecord();

    if (record == null) {
      return false;
    }

    nextRecord.add(record);

    return true;
  }

  @Override
  public Record next() {
    if (nextRecord.isEmpty()) {
      hasNext();
    }

    return nextRecord.poll();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  private Record fetchNextRecord() {
    while (recordIterator.hasNext()) {
      Record next = recordIterator.next();

      if (next.snapshotId() > maxSnapshotId) {
        continue;
      }

      boolean
          nextKeyEqualCurrent =
          currentKeyRecords.isEmpty() || next.key().equals(currentKeyRecords.last().key());

      if (nextKeyEqualCurrent) {
        currentKeyRecords.add(next);
        continue;
      }

      Record newest = currentKeyRecords.last();
      currentKeyRecords.clear();
      currentKeyRecords.add(next);
      return newest;
    }

    if (currentKeyRecords.isEmpty()) {
      return null;
    }

    Record newest = currentKeyRecords.last();
    currentKeyRecords.clear();
    return newest;
  }
}
