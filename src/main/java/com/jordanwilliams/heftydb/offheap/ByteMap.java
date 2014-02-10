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

import com.jordanwilliams.heftydb.data.Key;
import com.jordanwilliams.heftydb.data.Value;
import com.jordanwilliams.heftydb.util.Sizes;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class ByteMap implements Offheap, Iterable<ByteMap.Entry> {

    private static final int PAGE_SIZE = 1024;

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
                memorySize += Sizes.LONG_SIZE;
                memorySize += Sizes.INT_SIZE;
                memorySize += entry.getValue().size();
                counter++;
            }

            Memory memory = Memory.allocate(memorySize, PAGE_SIZE);
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

                ByteBuffer keyData = key.data();
                for (int i = 0; i < key.size(); i++) {
                    memoryBuffer.put(keyData.get(i));
                }

                memoryBuffer.putLong(key.snapshotId());

                //Value
                memoryBuffer.putInt(value.size());

                ByteBuffer valueData = value.data();
                for (int i = 0; i < value.size(); i++) {
                    memoryBuffer.put(valueData.get(i));
                }

                key.data().rewind();
                value.data().rewind();
            }

            memoryBuffer.rewind();

            return memory;
        }
    }

    private class AscendingIterator implements Iterator<Entry> {

        private int currentEntryIndex;

        public AscendingIterator(int startIndex) {
            this.currentEntryIndex = startIndex;
        }

        @Override
        public boolean hasNext() {
            return currentEntryIndex < entryCount;
        }

        @Override
        public Entry next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            Entry entry = getEntry(currentEntryIndex);
            currentEntryIndex++;
            return entry;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private class DescendingIterator implements Iterator<Entry> {

        private int currentEntryIndex;

        public DescendingIterator(int startIndex) {
            this.currentEntryIndex = startIndex;
        }

        @Override
        public boolean hasNext() {
            return currentEntryIndex > -1;
        }

        @Override
        public Entry next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            Entry entry = getEntry(currentEntryIndex);
            currentEntryIndex--;
            return entry;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
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

    public int ceilingIndex(Key key) {
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

        return low;
    }

    public int entryCount() {
        return entryCount;
    }

    public Iterator<Entry> ascendingIterator() {
        return new AscendingIterator(0);
    }

    public Iterator<Entry> ascendingIterator(Key key) {
        Key versionedKey = new Key(key.data(), 0);
        int startIndex = ceilingIndex(versionedKey);
        return new AscendingIterator(startIndex);
    }

    public Iterator<Entry> descendingIterator() {
        return new DescendingIterator(entryCount() - 1);
    }

    public Iterator<Entry> descendingIterator(Key key) {
        Key versionedKey = new Key(key.data(), Long.MAX_VALUE);
        int startIndex = floorIndex(versionedKey);
        return new DescendingIterator(startIndex);
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
        if (index < 0 || index >= entryCount) {
            throw new IndexOutOfBoundsException("Index: " + index + " Max: " + (entryCount - 1));
        }

        int entryOffset = entryOffset(index);

        //Key
        int keySize = directBuffer.getInt(entryOffset);

        ByteBuffer keyBuffer = ByteBuffer.allocate(keySize);
        int keyOffset = entryOffset + Sizes.INT_SIZE;

        for (int i = keyOffset; i < keyOffset + keySize; i++) {
            keyBuffer.put(directBuffer.get(i));
        }

        long snapshotId = directBuffer.getLong(keyOffset + keySize);

        keyBuffer.rewind();

        //Value
        int valueOffset = keyOffset + keySize + Sizes.LONG_SIZE;
        int valueSize = directBuffer.getInt(valueOffset);
        ByteBuffer valueBuffer = ByteBuffer.allocate(valueSize);
        valueOffset += Sizes.INT_SIZE;

        for (int i = valueOffset; i < valueOffset + valueSize; i++) {
            valueBuffer.put(directBuffer.get(i));
        }

        valueBuffer.rewind();

        return new Entry(new Key(keyBuffer, snapshotId), new Value(valueBuffer));
    }

    private int compareKeys(Key compareKey, int bufferKeyIndex) {
        int entryOffset = entryOffset(bufferKeyIndex);

        int keySize = directBuffer.getInt(entryOffset);
        entryOffset += Sizes.INT_SIZE;

        int bufferKeyRemaining = keySize;
        int compareKeyRemaining = compareKey.data().remaining();
        int compareCount = Math.min(bufferKeyRemaining, compareKeyRemaining);

        //Compare key bytes
        for (int i = 0; i < compareCount; i++) {
            byte bufferKeyVal = directBuffer.get(entryOffset + i);
            byte compareKeyVal = compareKey.data().get(i);
            bufferKeyRemaining--;
            compareKeyRemaining--;

            if (bufferKeyVal == compareKeyVal) {
                continue;
            }

            if (bufferKeyVal < compareKeyVal) {
                return -1;
            }

            return 1;
        }

        int remainingDifference = bufferKeyRemaining - compareKeyRemaining;

        //If key bytes are equal, compare snapshot ids
        if (remainingDifference == 0) {
            long bufferSnapshotId = directBuffer.getLong(entryOffset + compareCount);
            return Long.compare(bufferSnapshotId, compareKey.snapshotId());
        }

        return remainingDifference;
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
