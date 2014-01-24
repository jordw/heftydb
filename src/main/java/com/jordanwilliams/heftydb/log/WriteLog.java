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

package com.jordanwilliams.heftydb.log;

import com.jordanwilliams.heftydb.io.DataFile;
import com.jordanwilliams.heftydb.io.MutableDataFile;
import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.state.Paths;

import java.io.IOException;
import java.util.Iterator;

public class WriteLog implements Iterable<Record> {

    private final long tableId;
    private final DataFile dataFile;

    private WriteLog(long tableId, Paths paths) throws IOException {
        this.tableId = tableId;
        this.dataFile = MutableDataFile.open(paths.logPath(tableId));
    }

    public void append(Record record) throws IOException {

    }

    @Override
    public Iterator<Record> iterator() {
        return null;
    }

    public static WriteLog open(long tableId, Paths paths) throws IOException {
        return new WriteLog(tableId, paths);
    }
}
