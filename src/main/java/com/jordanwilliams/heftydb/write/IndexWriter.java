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
import com.jordanwilliams.heftydb.state.Files;
import com.jordanwilliams.heftydb.table.file.Index;

public class IndexWriter {

    private final long tableId;
    private final Files files;
    private final Index.Builder indexBuilder;

    private IndexWriter(long tableId, Files files) {
        this.tableId = tableId;
        this.files = files;
        this.indexBuilder = new Index.Builder();
    }

    public void addRecord(Record record) {

    }

    public static IndexWriter open(long tableId, Files files) {
        return new IndexWriter(tableId, files);
    }
}
