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

import com.jordanwilliams.heftydb.offheap.Memory;
import com.jordanwilliams.heftydb.record.Record;

import java.util.ArrayList;
import java.util.List;

public class DataBlockBuilder {

    private final List<Record> records = new ArrayList<Record>();
    private final int maxSizeBytes;
    private int sizeBytes;

    public DataBlockBuilder(int maxSizeBytes){
        this.maxSizeBytes = maxSizeBytes;
    }

    public void addRecord(Record record){
        records.add(record);
        sizeBytes += record.size();
    }

    public boolean isFull(){
        return sizeBytes >= maxSizeBytes;
    }

    public DataBlock build(){
        return null;
    }

    private static Memory serializeRecords(List<Record> records){
        return null;
    }
}
