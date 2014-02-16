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

import com.jordanwilliams.heftydb.util.ByteBuffers;

import java.nio.ByteBuffer;

public class Value implements Comparable<Value> {

    public static Value TOMBSTONE_VALUE = new Value(ByteBuffers.EMPTY_BUFFER);

    private final ByteBuffer value;

    public Value(ByteBuffer value) {
        this.value = value;
    }

    public ByteBuffer data() {
        return value;
    }

    public boolean isEmpty() {
        return value.capacity() == 0;
    }

    public int size() {
        return value.capacity();
    }

    @Override
    public int compareTo(Value o) {
        return value.compareTo(o.value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Value value1 = (Value) o;

        if (value != null ? !value.equals(value1.value) : value1.value != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return value != null ? value.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "Value{" +
                "data=" + new String(value.array()) +
                "}";
    }
}
