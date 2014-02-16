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

package com.jordanwilliams.heftydb.db;

import com.jordanwilliams.heftydb.compact.CompactionStrategies;
import com.jordanwilliams.heftydb.compact.CompactionStrategy;

import java.nio.file.Path;

public class Config {

    public static class Builder {

        private CompactionStrategy compactionStrategy = CompactionStrategies.SIZE_TIERED_COMPACTION_STRATEGY;
        private int memoryTableSize = 4096000;
        private int tableBlockSize = 32768;
        private int indexBlockSize = 32768;
        private int tableWriterThreads = 2;
        private int tableCompactionThreads = 4;
        private long tableCacheSize = 64000000;
        private long indexCacheSize = 64000000;
        private long maxCompactionRate = 16384000;
        private boolean printMetrics = false;
        private Path tableDirectory;
        private Path logDirectory;

        public Builder memoryTableSize(int memoryTableSize) {
            this.memoryTableSize = memoryTableSize;
            return this;
        }

        public Builder tableBlockSize(int tableBlockSize) {
            this.tableBlockSize = tableBlockSize;
            return this;
        }

        public Builder indexBlockSize(int indexBlockSize) {
            this.indexBlockSize = indexBlockSize;
            return this;
        }

        public Builder compactionStrategy(CompactionStrategy compactionStrategy) {
            this.compactionStrategy = compactionStrategy;
            return this;
        }

        public Builder tableWriterThreads(int tableWriterThreads) {
            this.tableWriterThreads = tableWriterThreads;
            return this;
        }

        public Builder tableCompactionThreads(int tableCompactionThreads) {
            this.tableCompactionThreads = tableCompactionThreads;
            return this;
        }

        public Builder tableCacheSize(long tableCacheSize) {
            this.tableCacheSize = tableCacheSize;
            return this;
        }

        public Builder indexCacheSize(long indexCacheSize) {
            this.indexCacheSize = indexCacheSize;
            return this;
        }

        public Builder printMetrics(boolean printMetrics) {
            this.printMetrics = printMetrics;
            return this;
        }

        public Builder directory(Path directory) {
            this.tableDirectory = directory;
            this.logDirectory = directory;
            return this;
        }

        public Builder tableDirectory(Path tableDirectory) {
            this.tableDirectory = tableDirectory;
            return this;
        }

        public Builder logDirectory(Path logDirectory) {
            this.logDirectory = logDirectory;
            return this;
        }

        public Builder maxCompactionRate(long maxCompactionRate){
            this.maxCompactionRate = maxCompactionRate;
            return this;
        }

        public Config build() {
            return new Config(compactionStrategy, memoryTableSize, tableBlockSize, indexBlockSize,
                    tableWriterThreads, tableCompactionThreads, tableCacheSize, indexCacheSize, printMetrics,
                    tableDirectory, logDirectory, maxCompactionRate);
        }
    }

    private final CompactionStrategy compactionStrategy;
    private final int memoryTableSize;
    private final int tableBlockSize;
    private final int indexBlockSize;
    private final int tableWriterThreads;
    private final int tableCompactionThreads;
    private final long tableCacheSize;
    private final long indexCacheSize;
    private final boolean printMetrics;
    private final Path tableDirectory;
    private final Path logDirectory;
    private final long maxCompactionRate;

    public Config(CompactionStrategy compactionStrategy, int memoryTableSize, int tableBlockSize, int indexBlockSize,
                  int tableWriterThreads, int tableCompactionThreads, long tableCacheSize, long indexCacheSize,
                  boolean printMetrics, Path tableDirectory, Path logDirectory, long maxCompactionRate) {
        this.compactionStrategy = compactionStrategy;
        this.memoryTableSize = memoryTableSize;
        this.tableBlockSize = tableBlockSize;
        this.indexBlockSize = indexBlockSize;
        this.tableWriterThreads = tableWriterThreads;
        this.tableCompactionThreads = tableCompactionThreads;
        this.tableCacheSize = tableCacheSize;
        this.indexCacheSize = indexCacheSize;
        this.printMetrics = printMetrics;
        this.tableDirectory = tableDirectory;
        this.logDirectory = logDirectory;
        this.maxCompactionRate = maxCompactionRate;
    }

    public CompactionStrategy compactionStrategy() {
        return compactionStrategy;
    }

    public int memoryTableSize() {
        return memoryTableSize;
    }

    public int tableBlockSize() {
        return tableBlockSize;
    }

    public int indexBlockSize() {
        return indexBlockSize;
    }

    public int tableWriterThreads() {
        return tableWriterThreads;
    }

    public int tableCompactionThreads() {
        return tableCompactionThreads;
    }

    public long tableCacheSize() {
        return tableCacheSize;
    }

    public long indexCacheSize() {
        return indexCacheSize;
    }

    public boolean printMetrics() {
        return printMetrics;
    }

    public Path tableDirectory() {
        return tableDirectory;
    }

    public Path logDirectory() {
        return logDirectory;
    }

    public long maxCompactionRate(){
        return maxCompactionRate;
    }
}
