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

public class SortedByteMap implements Offheap, Iterable<SortedByteMap.Entry> {

    public static class Entry {

        private final Key key;
        private final Value value;

        public Entry(Key key, Value value) {
            this.key = key;
            this.value = value;
        }

        public Key key(){
            return key;
        }

        public Value value(){
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

        private final Map<Key, Value> items = new LinkedHashMap<Key, Value>();

        public void add(Key key, Value value){
            items.put(key, value);
        }

        public SortedByteMap build(){
            return new SortedByteMap(serializeItems());
        }

        private Memory serializeItems(){
            //Allocate memory
            int memorySize = 0;
            int[] itemOffsets = new int[items.size()];

            memorySize += Sizes.INT_SIZE; //Pointer count
            memorySize += Sizes.INT_SIZE * items.size(); //Pointers

            //Compute memory size
            for (int i = 0; i < items.size(); i++) {
                itemOffsets[i] = memorySize;
            }

            Memory memory = Memory.allocate(memorySize);
            ByteBuffer memoryBuffer = memory.directBuffer();

            //Pack pointers
            memoryBuffer.putInt(items.size());

            for (int i = 0; i < itemOffsets.length; i++) {
                memoryBuffer.putInt(itemOffsets[i]);
            }

            //Pack items
            for (Map.Entry<Key, Value> item : items.entrySet()) {
                Key key = item.getKey();
                Value value = item.getValue();

                memoryBuffer.putInt(key.size());
                memoryBuffer.put(key.data());
                memoryBuffer.putInt(value.size());
                memoryBuffer.put(value.data());
            }

            return memory;
        }
    }

    private final Memory memory;
    private final ByteBuffer directBuffer;
    private final int itemCount;

    public SortedByteMap(Memory memory){
        this.memory = memory;
        this.directBuffer = memory.directBuffer();
        this.itemCount = memory.directBuffer().getInt(0);
    }

    public Entry get(int index){
        return getEntry(index);
    }

    public int floorIndex(Key key){
        int low = 0;
        int high = itemCount - 1;

        //Binary search
        while (low <= high) {
            int mid = (low + high) >>> 1;
            Key compareKey = getKey(mid);
            int compare = compareKey.compareTo(key);

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

    @Override
    public Iterator<Entry> iterator() {
        return new Iterator<Entry>() {

            int currentItemIndex = 0;

            @Override
            public boolean hasNext() {
                return currentItemIndex < itemCount;
            }

            @Override
            public Entry next() {
                if (currentItemIndex >= itemCount) {
                    throw new NoSuchElementException();
                }

                return getEntry(currentItemIndex);
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
        List<Entry> items = new ArrayList<Entry>();
        for (Entry item : this) {
            items.add(item);
        }

        return "SortedByteMap{items=" + items + "}";
    }

    private Entry getEntry(int index){
        int itemOffset = itemOffset(index);

        //Key
        int keySize = directBuffer.getInt(itemOffset);
        ByteBuffer keyBuffer = ByteBuffer.allocate(keySize);
        int keyOffset = itemOffset + Sizes.INT_SIZE;

        for (int i = keyOffset; i < keyOffset + keySize; i++){
            keyBuffer.put(directBuffer.get(i));
        }

        keyBuffer.rewind();

        //Value
        int valueOffset = keyOffset + keySize;
        int valueSize = directBuffer.getInt(valueOffset);
        ByteBuffer valueBuffer = ByteBuffer.allocate(valueSize);
        valueOffset += Sizes.INT_SIZE;

        for (int i = valueOffset; i < valueOffset + valueSize; i++){
            valueBuffer.put(directBuffer.get(i));
        }

        valueBuffer.rewind();

        return new Entry(new Key(keyBuffer), new Value(valueBuffer));
    }

    private Key getKey(int index){
        int itemOffset = itemOffset(index);

        //Key
        int keySize = directBuffer.getInt(itemOffset);
        ByteBuffer keyBuffer = ByteBuffer.allocate(keySize);
        int keyOffset = itemOffset + Sizes.INT_SIZE;

        for (int i = keyOffset; i < keyOffset + keySize; i++){
            keyBuffer.put(directBuffer.get(i));
        }

        keyBuffer.rewind();

        return new Key(keyBuffer);
    }

    private int itemOffset(int index) {
        return directBuffer.getInt(pointerOffset(index));
    }

    private static int pointerOffset(int pointerIndex) {
        int pointerOffset = Sizes.INT_SIZE;
        pointerOffset += pointerIndex * Sizes.INT_SIZE;
        return pointerOffset;
    }
}
