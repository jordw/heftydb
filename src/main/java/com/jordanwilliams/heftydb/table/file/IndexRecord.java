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

import com.jordanwilliams.heftydb.record.Key;
import com.jordanwilliams.heftydb.util.Serializer;
import com.jordanwilliams.heftydb.util.Sizes;

import java.nio.ByteBuffer;

public class IndexRecord implements Comparable<IndexRecord> {

    public static final Serializer.ByteBufferSerializer<IndexRecord> SERIALIZER = new Serializer.ByteBufferSerializer<IndexRecord>() {
        @Override
        public ByteBuffer serialize(IndexRecord data) {
            ByteBuffer buffer = ByteBuffer.allocate(serializedSize(data));

            //Start key
            ByteBuffer startKeyBuffer = Key.SERIALIZER.serialize(data.startKey);
            buffer.putInt(startKeyBuffer.capacity());
            buffer.put(startKeyBuffer);

            //Offset
            buffer.putLong(data.offset);

            return buffer;
        }

        @Override
        public IndexRecord deserialize(ByteBuffer in) {
            ByteBuffer buffer = in.duplicate();
            buffer.rewind();

            //Start key
            int startKeyLength = buffer.getInt();
            Key startKey = Key.SERIALIZER.deserialize(ByteBuffer.wrap(buffer.array(), buffer.position(),
                    startKeyLength).slice());
            buffer.position(buffer.position() + startKeyLength);

            //Offset
            long offset = buffer.getLong();

            return new IndexRecord(startKey, offset);
        }

        @Override
        public int serializedSize(IndexRecord data) {
            return Sizes.INT_SIZE + //Start key length
                   Key.SERIALIZER.serializedSize(data.startKey) + //Start key
                   Sizes.LONG_SIZE; //Offset
        }
    };

    private final Key startKey;
    private final long offset;

    public IndexRecord(Key startKey, long offset) {
        this.startKey = startKey;
        this.offset = offset;
    }

    public Key startKey() {
        return startKey;
    }

    public long offset() {
        return offset;
    }

    @Override
    public int compareTo(IndexRecord o) {
        return startKey.compareTo(o.startKey);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexRecord that = (IndexRecord) o;

        if (offset != that.offset) return false;
        if (startKey != null ? !startKey.equals(that.startKey) : that.startKey != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = startKey != null ? startKey.hashCode() : 0;
        result = 31 * result + (int) (offset ^ (offset >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "IndexRecord{" +
                "startKey=" + startKey +
                ", offset=" + offset +
                '}';
    }
}
