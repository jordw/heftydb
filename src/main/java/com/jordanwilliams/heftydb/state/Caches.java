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

package com.jordanwilliams.heftydb.state;

import com.jordanwilliams.heftydb.index.IndexBlock;
import com.jordanwilliams.heftydb.table.file.DataBlock;

public class Caches {

    private final DataBlock.Cache recordBlockCache;
    private final IndexBlock.Cache indexBlockCache;

    public Caches(DataBlock.Cache recordBlockCache, IndexBlock.Cache indexBlockCache) {
        this.recordBlockCache = recordBlockCache;
        this.indexBlockCache = indexBlockCache;
    }

    public DataBlock.Cache recordBlockCache() {
        return recordBlockCache;
    }

    public IndexBlock.Cache indexBlockCache() {
        return indexBlockCache;
    }
}
