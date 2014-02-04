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

package com.jordanwilliams.heftydb.test.generator;

import com.jordanwilliams.heftydb.state.Caches;
import com.jordanwilliams.heftydb.state.Config;
import com.jordanwilliams.heftydb.state.Paths;
import com.jordanwilliams.heftydb.state.State;
import com.jordanwilliams.heftydb.index.IndexBlock;
import com.jordanwilliams.heftydb.table.file.DataBlock;
import com.jordanwilliams.heftydb.test.helper.TestFileHelper;

import java.nio.file.Path;
import java.util.Collections;

public class ConfigGenerator {

    public static Paths testPaths() {
        return new Paths(TestFileHelper.TEMP_PATH, TestFileHelper.TEMP_PATH);
    }

    public static Caches testCaches() {
        return new Caches(new DataBlock.Cache(32768000), new IndexBlock.Cache(16384000));
    }

    public static Config testConfig() {
        return new Config() {
            @Override
            public int memoryTableSize() {
                return 16384;
            }

            @Override
            public int fileTableBlockSize() {
                return 4096;
            }

            @Override
            public int indexBlockSize() {
                return 4096;
            }

            @Override
            public int tableWriterThreads() {
                return 1;
            }

            @Override
            public int tableCompactionThreads() {
                return 1;
            }

            @Override
            public long tableCacheSize() {
                return 1024000;
            }

            @Override
            public long indexCacheSize() {
                return 1024000;
            }

            @Override
            public Path tableDirectory() {
                return TestFileHelper.TEMP_PATH;
            }

            @Override
            public Path logDirectory() {
                return TestFileHelper.TEMP_PATH;
            }
        };
    }

    public static State testState(){
        return new State(Collections.EMPTY_LIST, testConfig(), testPaths(), testCaches(), 1);
    }
}
