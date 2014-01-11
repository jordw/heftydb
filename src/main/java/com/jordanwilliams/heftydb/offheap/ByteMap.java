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

package com.jordanwilliams.heftydb.offheap;

import com.jordanwilliams.heftydb.record.Key;
import com.jordanwilliams.heftydb.record.Value;
import com.jordanwilliams.heftydb.util.Sizes;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class ByteMap implements Offheap, Iterable<ByteMap.Entry> {

    public static class Entry {

        private final Key key;
        private final Value value;

        public Entry(Key key, Value value) {
            this.key = key;
            this.value = value;
        }

        public Key key() {
            return key;
        }

        public Value value() {
            return value;
        }

        @Override
        public String toString() {
            return "Entry{" +
                    "key=" + key +
                    ", value=" + value +
                    '}';
        }
    }

    public static class Builder {

        private final Map<Key, Value> entries = new LinkedHashMap<Key, Value>();

        public void add(Key key, Value value) {
            entries.put(key, value);
        }

        public ByteMap build() {
            return new ByteMap(serializeEntries());
        }

        private Memory serializeEntries() {
            //Allocate memory
            int memorySize = 0;
            int[] entryOffsets = new int[entries.size()];

            memorySize += Sizes.INT_SIZE; //Pointer count
            memorySize += Sizes.INT_SIZE * entries.size(); //Pointers

            //Compute memory size
            int counter = 0;

            for (Map.Entry<Key, Value> entry : entries.entrySet()) {
                entryOffsets[counter] = memorySize;
                memorySize += Sizes.INT_SIZE;
                memorySize += entry.getKey().size();
                memorySize += Sizes.INT_SIZE;
                memorySize += entry.getValue().size();
                counter++;
            }

            Memory memory = Memory.allocate(memorySize);
            ByteBuffer memoryBuffer = memory.directBuffer();

            //Pack pointers
            memoryBuffer.putInt(entries.size());

            for (int i = 0; i < entryOffsets.length; i++) {
                memoryBuffer.putInt(entryOffsets[i]);
            }

            //Pack entries
            for (Map.Entry<Key, Value> entry : entries.entrySet()) {
                Key key = entry.getKey();
                Value value = entry.getValue();

                key.data().rewind();
                value.data().rewind();

                //Key
                memoryBuffer.putInt(key.size());
                memoryBuffer.put(key.data());

                //Value
                memoryBuffer.putInt(value.size());
                memoryBuffer.put(value.data());

                key.data().rewind();
                value.data().rewind();
            }

            return memory;
        }
    }

    private final Memory memory;
    private final ByteBuffer directBuffer;
    private final int entryCount;

    public ByteMap(Memory memory) {
        this.memory = memory;
        this.directBuffer = memory.directBuffer();
        this.entryCount = memory.directBuffer().getInt(0);
    }

    public Entry get(int index) {
        return getEntry(index);
    }

    public int floorIndex(Key key) {
        int low = 0;
        int high = entryCount - 1;

        //Binary search
        while (low <= high) {
            int mid = (low + high) >>> 1;
            int compare = compareKeys(key, mid);

            if (compare < 0) {
                low = mid + 1;
            } else if (compare > 0) {
                high = mid - 1;
            } else {
                return mid;
            }
        }

        return low - 1;
    }

    public int entryCount() {
        return entryCount;
    }

    @Override
    public Iterator<Entry> iterator() {
        return new Iterator<Entry>() {

            int currentEntryIndex = 0;

            @Override
            public boolean hasNext() {
                return currentEntryIndex < entryCount;
            }

            @Override
            public Entry next() {
                if (currentEntryIndex >= entryCount) {
                    throw new NoSuchElementException();
                }

                return getEntry(currentEntryIndex++);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public Memory memory() {
        return memory;
    }

    @Override
    public String toString() {
        List<Entry> entries = new ArrayList<Entry>();
        for (Entry entry : this) {
            entries.add(entry);
        }

        return "ByteMap{entries=" + entries + "}";
    }

    private Entry getEntry(int index) {
        int entryOffset = entryOffset(index);

        //Key
        int keySize = directBuffer.getInt(entryOffset);
        ByteBuffer keyBuffer = ByteBuffer.allocate(keySize);
        int keyOffset = entryOffset + Sizes.INT_SIZE;

        for (int i = keyOffset; i < keyOffset + keySize; i++) {
            keyBuffer.put(directBuffer.get(i));
        }

        keyBuffer.rewind();

        //Value
        int valueOffset = keyOffset + keySize;
        int valueSize = directBuffer.getInt(valueOffset);
        ByteBuffer valueBuffer = ByteBuffer.allocate(valueSize);
        valueOffset += Sizes.INT_SIZE;

        for (int i = valueOffset; i < valueOffset + valueSize; i++) {
            valueBuffer.put(directBuffer.get(i));
        }

        valueBuffer.rewind();

        return new Entry(new Key(keyBuffer), new Value(valueBuffer));
    }

    private Key getKey(int index) {
        int entryOffset = entryOffset(index);

        //Key
        int keySize = directBuffer.getInt(entryOffset);
        ByteBuffer keyBuffer = ByteBuffer.allocate(keySize);
        int keyOffset = entryOffset + Sizes.INT_SIZE;

        for (int i = keyOffset; i < keyOffset + keySize; i++) {
            keyBuffer.put(directBuffer.get(i));
        }

        keyBuffer.rewind();

        return new Key(keyBuffer);
    }

    private int compareKeys(Key key, int index) {
        int entryOffset = entryOffset(index);
        int keySize = directBuffer.getInt(entryOffset);
        entryOffset += Sizes.INT_SIZE;

        int compareCount = Math.min(keySize, key.data().remaining());
        int remaining = keySize;

        for (int i = 0; i < compareCount; i++) {
            byte thisVal = directBuffer.get(entryOffset + i);
            byte thatVal = key.data().get(i);
            remaining--;

            if (thisVal == thatVal) {
                continue;
            }

            if (thisVal < thatVal) {
                return -1;
            }

            return 1;
        }

        return remaining - key.data().remaining();
    }

    private int entryOffset(int index) {
        return directBuffer.getInt(pointerOffset(index));
    }

    private static int pointerOffset(int pointerIndex) {
        int pointerOffset = Sizes.INT_SIZE;
        pointerOffset += pointerIndex * Sizes.INT_SIZE;
        return pointerOffset;
    }
}
