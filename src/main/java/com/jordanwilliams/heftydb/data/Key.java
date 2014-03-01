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

import java.nio.ByteBuffer;

public class Key implements Comparable<Key> {

    private final ByteBuffer data;
    private final long snapshotId;

    public Key(ByteBuffer data, long snapshotId) {
        this.data = data;
        this.snapshotId = snapshotId;
    }

    public ByteBuffer data() {
        return data;
    }

    public long snapshotId() {
        return snapshotId;
    }

    public int size() {
        return data.capacity();
    }

    @Override
    public int compareTo(Key o) {
        int compared = data.compareTo(o.data);

        if (compared != 0) {
            return compared;
        }

        return Long.compare(snapshotId, o.snapshotId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Key key = (Key) o;

        if (snapshotId != key.snapshotId) return false;
        if (data != null ? !data.equals(key.data) : key.data != null) return false;

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
                ", snapshotId=" + snapshotId +
                '}';
    }
}