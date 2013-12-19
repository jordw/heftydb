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
import com.jordanwilliams.heftydb.util.FilteringIterator;

import java.util.Iterator;

public class VersionedRecordIterator implements Iterator<Record> {

    private static class SnapshotFilter implements FilteringIterator.Filter<Record> {

        private final long maxSnapshotId;
        private Record lastSeenRecord;

        private SnapshotFilter(long maxSnapshotId) {
            this.maxSnapshotId = maxSnapshotId;
        }

        @Override
        public Record next(Iterator<Record> delegate) {
            Record newestRecord = null;

            while (delegate.hasNext()) {
                Record currentRecord;

                if (lastSeenRecord != null) {
                    currentRecord = lastSeenRecord;
                    lastSeenRecord = null;
                } else {
                    currentRecord = delegate.next();
                }

                //If we have seen the next unique key, then we can be sure we have the latest
                //version of the previous key, so we can return it
                if (newestRecord != null && !currentRecord.key().equals(newestRecord.key())) {
                    lastSeenRecord = currentRecord;
                    return newestRecord;
                }

                //Make sure we keep only the latest version of the key when there are multiple
                //versions found
                if (newestRecord == null || currentRecord.snapshotId() > newestRecord.snapshotId()) {
                    newestRecord = currentRecord;
                }
            }

            return lastSeenRecord;
        }
    }

    private final FilteringIterator<Record> filteringIterator;

    public VersionedRecordIterator(long maxSnapshotId, Iterator<Record> delegate) {
        this.filteringIterator = new FilteringIterator<Record>(new SnapshotFilter(maxSnapshotId), delegate);
    }

    @Override
    public boolean hasNext() {
        return filteringIterator.hasNext();
    }

    @Override
    public Record next() {
        return filteringIterator.next();
    }

    @Override
    public void remove() {
        filteringIterator.remove();
    }
}
