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
            ByteBuffer serialized = data.key.duplicate();
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
            return data.key.capacity();
        }
    };

    private final ByteBuffer key;

    public Key(ByteBuffer key) {
        this.key = key;
    }

    public ByteBuffer key(){
        return key;
    }

    public int size(){
        return key.capacity();
    }

    @Override
    public int compareTo(Key o) {
        return key.compareTo(o.key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Key key1 = (Key) o;

        if (key != null ? !key.equals(key1.key) : key1.key != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return key != null ? key.hashCode() : 0;
    }

    @Override
    public String toString() {
        byte[] keyArray = new byte[key.capacity()];
        key.rewind();
        key.get(keyArray);
        key.rewind();

        return "Key{" +
                "key=" + new String(keyArray) +
                '}';
    }
}
