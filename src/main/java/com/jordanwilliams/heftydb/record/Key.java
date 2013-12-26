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
import net.jcip.annotations.Immutable;

import java.nio.ByteBuffer;

@Immutable
public class Key implements Comparable<Key> {

    public static final Serializer.ByteBufferSerializer<Key> SERIALIZER = new Serializer.ByteBufferSerializer<Key>() {
        @Override
        public ByteBuffer serialize(Key data) {
            ByteBuffer serialized = data.data.duplicate();
            serialized.rewind();
            return serialized;
        }

        @Override
        public Key deserialize(ByteBuffer in) {
            ByteBuffer backingBuffer = in.duplicate();
            backingBuffer.rewind();
            return new Key(backingBuffer);
        }

        @Override
        public int serializedSize(Key data) {
            return data.data.capacity();
        }
    };

    private final ByteBuffer data;

    public Key(ByteBuffer key) {
        this.data = key;
    }

    public ByteBuffer key() {
        return data;
    }

    public int size() {
        return data.capacity();
    }

    @Override
    public int compareTo(Key o) {
        return data.compareTo(o.data);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Key key1 = (Key) o;

        if (data != null ? !data.equals(key1.data) : key1.data != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return data != null ? data.hashCode() : 0;
    }

    @Override
    public String toString() {
        byte[] keyArray = new byte[data.capacity()];
        data.rewind();
        data.get(keyArray);
        data.rewind();

        return "Key{" +
                "data=" + new String(keyArray) +
                '}';
    }
}
