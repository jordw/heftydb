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

package com.jordanwilliams.heftydb.table.file;

import com.jordanwilliams.heftydb.offheap.ByteMap;
import com.jordanwilliams.heftydb.offheap.Memory;
import com.jordanwilliams.heftydb.offheap.Offheap;
import com.jordanwilliams.heftydb.record.Key;
import com.jordanwilliams.heftydb.record.Value;
import com.jordanwilliams.heftydb.util.Sizes;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class IndexBlock implements Offheap {

    public static class Builder {

        private final ByteMap.Builder byteMapBuilder = new ByteMap.Builder();

        private int sizeBytes;

        public void addRecord(IndexRecord indexRecord) {
            byteMapBuilder.add(new Key(indexRecord.startKey().data(), indexRecord.snapshotId()),
                    new Value(indexRecordValue(indexRecord)));
            sizeBytes += indexRecord.sizeBytes();
        }

        public int sizeBytes() {
            return sizeBytes;
        }

        public IndexBlock build() {
            return new IndexBlock(byteMapBuilder.build());
        }

        private ByteBuffer indexRecordValue(IndexRecord indexRecord){
            ByteBuffer contentsBuffer = ByteBuffer.allocate(indexRecord.contentsSizeBytes());
            contentsBuffer.putLong(indexRecord.offset());
            contentsBuffer.put(indexRecord.isLeaf() ? (byte) 1 : (byte) 0);
            contentsBuffer.rewind();
            return contentsBuffer;
        }
    }

    private final ByteMap byteMap;

    public IndexBlock(ByteMap byteMap) {
        this.byteMap = byteMap;
    }

    public IndexRecord startRecord() {
        return deserializeRecord(0);
    }

    public IndexRecord get(Key key, long maxSnapshotId){
        int closestIndex = byteMap.floorIndex(new Key(key.data(), maxSnapshotId));

        if (closestIndex < 0){
            return null;
        }

        if (closestIndex >= byteMap.entryCount()){
            closestIndex = byteMap.entryCount() - 1;
        }

        IndexRecord indexRecord = deserializeRecord(closestIndex);
        return indexRecord;
    }

    @Override
    public Memory memory() {
        return byteMap.memory();
    }

    private IndexRecord deserializeRecord(int recordIndex){
        ByteMap.Entry entry = byteMap.get(recordIndex);
        ByteBuffer entryKeyBuffer = entry.key().data();
        ByteBuffer recordKeyBuffer = ByteBuffer.allocate(entryKeyBuffer.capacity() - Sizes.LONG_SIZE);

        for (int i = 0; i < recordKeyBuffer.capacity(); i++){
            recordKeyBuffer.put(i, entryKeyBuffer.get(i));
        }

        long snapshotId = entryKeyBuffer.getLong(entryKeyBuffer.capacity() - Sizes.LONG_SIZE);
        long offset = entry.value().data().getLong(0);
        boolean isLeaf = entry.value().data().get(Sizes.LONG_SIZE) == (byte) 1;

        return new IndexRecord(new Key(recordKeyBuffer), snapshotId, offset, isLeaf);
    }

    @Override
    public String toString() {
        List<IndexRecord> records = new ArrayList<IndexRecord>();
        return "IndexBlock{records=" + records + "}";
    }
}
