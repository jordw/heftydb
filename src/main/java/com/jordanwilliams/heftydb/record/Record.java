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

package com.jordanwilliams.heftydb.record;

import com.jordanwilliams.heftydb.util.Serializer;
import com.jordanwilliams.heftydb.util.Sizes;
import net.jcip.annotations.Immutable;

import java.nio.ByteBuffer;

@Immutable
public final class Record implements Comparable<Record> {

    public static final Serializer.ByteBufferSerializer<Record> SERIALIZER = new Serializer.ByteBufferSerializer<Record>() {

        public int serializedSize(Record record) {
            return Sizes.INT_SIZE + //Key size
                    record.key().capacity() + //Key
                    Sizes.INT_SIZE + //Value size
                    (record.value() == null ? 0 : record.value().capacity()) + //Value
                    Sizes.LONG_SIZE; //Write Id
        }

        @Override
        public Record deserialize(ByteBuffer byteBuffer) {
            ByteBuffer buffer = byteBuffer.asReadOnlyBuffer();
            buffer.rewind();

            int keyLength = buffer.getInt();
            byte[] keyBytes = new byte[keyLength];
            buffer.get(keyBytes);

            int valueLength = buffer.getInt();
            byte[] valueBytes = new byte[valueLength];
            buffer.get(valueBytes);

            long writeId = buffer.getLong();

            return new Record(ByteBuffer.wrap(keyBytes), ByteBuffer.wrap(valueBytes), writeId);
        }

        @Override
        public ByteBuffer serialize(Record record) {
            ByteBuffer serializedBuffer = ByteBuffer.allocate(serializedSize(record));
            ByteBuffer keyDup = record.key.duplicate();
            ByteBuffer valueDup = record.value.duplicate();

            keyDup.rewind();
            valueDup.rewind();

            serializedBuffer.putInt(keyDup.capacity());
            serializedBuffer.put(keyDup);
            serializedBuffer.putInt(valueDup.capacity());
            serializedBuffer.put(valueDup);
            serializedBuffer.putLong(record.snapshotId());

            serializedBuffer.rewind();

            return serializedBuffer;
        }
    };

    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    private final ByteBuffer key;
    private final ByteBuffer value;
    private final long snapshotId;

    public Record(ByteBuffer key, ByteBuffer value, long snapshotId) {
        this.key = key;
        this.value = value == null ? EMPTY_BUFFER : value;
        this.snapshotId = snapshotId;
    }

    public boolean tombstone() {
        return value.capacity() == 0;
    }

    public ByteBuffer key() {
        return key.duplicate();
    }

    public ByteBuffer value() {
        return value.duplicate();
    }

    public long snapshotId() {
        return snapshotId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Record that = (Record) o;

        if (snapshotId != that.snapshotId) return false;
        if (key != null ? !key.equals(that.key) : that.key != null) return false;
        if (value != null ? !value.equals(that.value) : that.value != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (int) (snapshotId ^ (snapshotId >>> 32));
        return result;
    }

    @Override
    public int compareTo(Record other) {
        int compared = key.compareTo(other.key);

        if (compared != 0){
            return compared;
        }

        if (snapshotId == other.snapshotId){
            return 0;
        }

        return snapshotId > other.snapshotId ? 1 : -1;
    }

    @Override
    public String toString() {
        return "Record{" +
                "key=" + new String(key.array()) +
                ", value=" + value +
                ", snapshotId=" + snapshotId +
                '}';
    }
}
