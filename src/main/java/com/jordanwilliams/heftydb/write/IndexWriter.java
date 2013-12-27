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

package com.jordanwilliams.heftydb.write;

import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.state.DataFiles;
import com.jordanwilliams.heftydb.table.file.Index;

public class IndexWriter {

    private final long tableId;
    private final DataFiles dataFiles;
    private final Index.Builder indexBuilder;

    private IndexWriter(long tableId, DataFiles dataFiles) {
        this.tableId = tableId;
        this.dataFiles = dataFiles;
        this.indexBuilder = new Index.Builder();
    }

    public void addRecord(Record record) {

    }

    public static IndexWriter open(long tableId, DataFiles dataFiles) {
        return new IndexWriter(tableId, dataFiles);
    }
}
