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

package com.jordanwilliams.heftydb.data;

import com.jordanwilliams.heftydb.util.Serializer;
import com.jordanwilliams.heftydb.util.Sizes;

import java.nio.ByteBuffer;

public class Tuple implements Comparable<Tuple> {

    public static Serializer<Tuple> SERIALIZER = new Serializer<Tuple>() {
        @Override
        public int size(Tuple tuple) {
            int size = 0;

            //Key
            size += Sizes.INT_SIZE;
            size += tuple.key().size();
            size += Sizes.LONG_SIZE;

            //Value
            size += Sizes.INT_SIZE;
            size += tuple.value().size();

            return size;
        }

        @Override
        public void serialize(Tuple tuple, ByteBuffer recordBuffer) {
            //Key
            recordBuffer.putInt(tuple.key.size());
            recordBuffer.put(tuple.key.data());
            recordBuffer.putLong(tuple.key.snapshotId());
            tuple.key().data().rewind();

            //Value
            recordBuffer.putInt(tuple.value.size());
            recordBuffer.put(tuple.value().data());
            tuple.value().data().rewind();

            recordBuffer.rewind();
        }

        @Override
        public Tuple deserialize(ByteBuffer recordBuffer) {
            //Key
            int keySize = recordBuffer.getInt();
            ByteBuffer keyBuffer = ByteBuffer.allocate(keySize);
            recordBuffer.get(keyBuffer.array());
            long snapshotId = recordBuffer.getLong();
            Key key = new Key(keyBuffer, snapshotId);

            //Value
            int valueSize = recordBuffer.getInt();
            ByteBuffer valueBuffer = ByteBuffer.allocate(valueSize);
            recordBuffer.get(valueBuffer.array());
            Value value = new Value(valueBuffer);

            return new Tuple(key, value);
        }
    };

    private final Key key;
    private final Value value;

    public Tuple(Key key, Value value) {
        this.key = key;
        this.value = value;
    }

    public Key key() {
        return key;
    }

    public Value value() {
        return value;
    }

    public int size() {
        return key.size() + value().size();
    }

    @Override
    public int compareTo(Tuple o) {
        return key.compareTo(o.key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Tuple tuple = (Tuple) o;

        if (key != null ? !key.equals(tuple.key) : tuple.key != null) return false;
        if (value != null ? !value.equals(tuple.value) : tuple.value != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Tuple{" +
                "key=" + key +
                ", value=" + value +
                '}';
    }
}
