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

package com.jordanwilliams.heftydb.write;

import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.io.AppendChannelFile;
import com.jordanwilliams.heftydb.io.AppendFile;
import com.jordanwilliams.heftydb.state.Paths;
import com.jordanwilliams.heftydb.util.XORShiftRandom;

import java.io.Closeable;
import java.io.IOException;

public class CommitLogWriter implements Closeable {

    private final long tableId;
    private final XORShiftRandom pseudoRandom;
    private final AppendFile logFile;

    private CommitLogWriter(long tableId, AppendFile logFile) throws IOException {
        long seed = System.nanoTime();
        this.tableId = tableId;
        this.pseudoRandom = new XORShiftRandom(seed);
        this.logFile = logFile;

        logFile.appendLong(seed);
    }

    public void append(Tuple tuple, boolean fsync) throws IOException {
        int serializedSize = Tuple.SERIALIZER.size(tuple);

        //Serialize in place to avoid an extra copy
        tuple.rewind();
        logFile.appendInt(serializedSize);

        logFile.appendInt(tuple.key().size());
        logFile.append(tuple.key().data());
        logFile.appendLong(tuple.key().snapshotId());

        logFile.appendInt(tuple.value().size());
        logFile.append(tuple.value().data());
        logFile.appendInt(pseudoRandom.nextInt());
        tuple.rewind();

        if (fsync) {
            logFile.sync();
        }
    }

    public long tableId() {
        return tableId;
    }

    @Override
    public void close() throws IOException {
        logFile.close();
    }

    public static CommitLogWriter open(long tableId, Paths paths) throws IOException {
        AppendFile logFile = AppendChannelFile.open(paths.logPath(tableId));
        return new CommitLogWriter(tableId, logFile);
    }
}
