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

package com.jordanwilliams.heftydb.table.file;

import com.jordanwilliams.heftydb.offheap.SortedByteMap;
import com.jordanwilliams.heftydb.record.Key;
import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.util.Sizes;

import java.nio.ByteBuffer;

public class RecordBlockNew {

    public static class Builder {

        private final SortedByteMap.Builder byteMapBuilder = new SortedByteMap.Builder();

        private int sizeBytes;

        public void add(Record record){
            Key versionedKey = versionedKey(record.key(), record.snapshotId());
            byteMapBuilder.add(versionedKey, record.value());
            sizeBytes += record.size();
        }

        public RecordBlockNew build(){
            return new RecordBlockNew(byteMapBuilder.build());
        }
    }

    private final SortedByteMap byteMap;

    public RecordBlockNew(SortedByteMap byteMap){
        this.byteMap = byteMap;
    }

    public Record get(Key key, long maxSnapshotId){
        return null;
    }

    private static Key versionedKey(Key key, long snapshotId) {
        ByteBuffer versionedKey = ByteBuffer.allocate(key.size() + Sizes.LONG_SIZE);
        versionedKey.put(key.data());
        versionedKey.putLong(snapshotId);
        versionedKey.rewind();
        return new Key(versionedKey);
    }
}
