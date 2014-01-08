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

import com.jordanwilliams.heftydb.offheap.Memory;
import com.jordanwilliams.heftydb.offheap.Offheap;
import com.jordanwilliams.heftydb.record.Key;
import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.record.Value;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class IndexBlock implements Offheap {

    public static class Builder {

        private final RecordBlock.Builder recordBlockBuilder = new RecordBlock.Builder();

        private int sizeBytes;

        public void addRecord(IndexRecord indexRecord) {
            recordBlockBuilder.addRecord(toRecord(indexRecord));
            sizeBytes += indexRecord.sizeBytes();
        }

        public int sizeBytes() {
            return sizeBytes;
        }

        public IndexBlock build() {
            return new IndexBlock(recordBlockBuilder.build());
        }

        private Record toRecord(IndexRecord indexRecord){
            return new Record(indexRecord.startKey(), new Value(indexRecordContents(indexRecord)),
                    indexRecord.snapshotId());
        }

        private ByteBuffer indexRecordContents(IndexRecord indexRecord){
            ByteBuffer contentsBuffer = ByteBuffer.allocate(indexRecord.contentsSizeBytes());
            contentsBuffer.order(ByteOrder.nativeOrder());
            contentsBuffer.putLong(indexRecord.offset());
            contentsBuffer.put(indexRecord.isLeaf() ? (byte) 1 : (byte) 0);
            contentsBuffer.rewind();
            return contentsBuffer;
        }
    }

    private final RecordBlock recordBlock;

    public IndexBlock(RecordBlock recordBlock) {
        this.recordBlock = recordBlock;
    }

    public IndexRecord startRecord() {
        return deserializeRecord(0);
    }

    public IndexRecord get(Key key, long maxSnapshotId){
        int closestIndex = recordBlock.closestRecordIndex(key, maxSnapshotId);

        if (closestIndex < 0){
            return null;
        }

        if (closestIndex >= recordBlock.recordCount()){
            closestIndex = recordBlock.recordCount() - 1;
        }

        IndexRecord indexRecord = deserializeRecord(closestIndex);
        return indexRecord;
    }

    @Override
    public Memory memory() {
        return recordBlock.memory();
    }

    private IndexRecord deserializeRecord(int recordIndex){
        return null;
    }

    @Override
    public String toString() {
        List<IndexRecord> records = new ArrayList<IndexRecord>();
        return "IndexBlock{records=" + records + "}";
    }
}
