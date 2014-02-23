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

package com.jordanwilliams.heftydb.test.generator;

import com.jordanwilliams.heftydb.compact.CompactionStrategies;
import com.jordanwilliams.heftydb.db.Config;
import com.jordanwilliams.heftydb.db.DBState;
import com.jordanwilliams.heftydb.index.IndexBlock;
import com.jordanwilliams.heftydb.metrics.Metrics;
import com.jordanwilliams.heftydb.state.Caches;
import com.jordanwilliams.heftydb.state.Paths;
import com.jordanwilliams.heftydb.table.Table;
import com.jordanwilliams.heftydb.table.file.TupleBlock;
import com.jordanwilliams.heftydb.test.helper.TestFileHelper;

import java.util.Collections;

public class ConfigGenerator {

    public static Paths testPaths() {
        return new Paths(TestFileHelper.TEMP_PATH, TestFileHelper.TEMP_PATH);
    }

    public static Caches testCaches() {
        return new Caches(new TupleBlock.Cache(32768000, new Metrics(testConfig())), new IndexBlock.Cache(16384000,
                new Metrics(testConfig())));
    }

    public static Config defaultConfig() {
        return new Config.Builder().tableDirectory(TestFileHelper.TEMP_PATH).build();
    }

    public static Config enduranceConfig() {
        return new Config.Builder().tableDirectory(TestFileHelper.TEMP_PATH).printMetrics(true).indexCacheSize
                (64000000).tableCacheSize(512000000).memoryTableSize(8192000).maxMemoryTableWriteRate(32768000)
                .maxCompactionRate(32768000).build();
    }

    public static Config performanceConfig() {
        return new Config.Builder().tableDirectory(TestFileHelper.TEMP_PATH).indexCacheSize
                (128000000).tableCacheSize(512000000).tableBlockSize(1024).memoryTableSize(8192000).maxCompactionRate
                (32768000).build();
    }

    public static Config testConfig() {
        Config.Builder builder = new Config.Builder();

        return builder.compactionStrategy(CompactionStrategies.NULL_COMPACTION_STRATEGY).memoryTableSize(16384)
                .tableBlockSize(4096).indexBlockSize(4096).tableCacheSize(1024000).indexCacheSize(1024000)
                .tableDirectory(TestFileHelper.TEMP_PATH).build();
    }

    public static DBState perfState() {
        return new DBState(Collections.<Table>emptyList(), defaultConfig(), testPaths(), testCaches(), 1);
    }
}
