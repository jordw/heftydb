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

import java.util.ArrayList;
import java.util.List;

public class IndexBuilder {

    public interface Callback {
        public long writeIndexBlock(IndexBlock completed);
    }

    private final Callback callback;
    private final List<IndexBlock.Builder> indexBlockBuilders = new ArrayList<IndexBlock.Builder>();
    private final int indexBlockSizeBytes;

    public IndexBuilder(Callback callback, int indexBlockSizeBytes){
        this.callback = callback;
        this.indexBlockSizeBytes = indexBlockSizeBytes;
    }

    public void add(IndexBlock.Record record){
        insert(record, 0);
    }

    private void insert(IndexBlock.Record record, int level){
        if (level == indexBlockBuilders.size()){
            indexBlockBuilders.add(new IndexBlock.Builder());
        }

        IndexBlock.Builder builder = indexBlockBuilders.get(level);

        if (builder.sizeBytes() >= indexBlockSizeBytes){
            IndexBlock completed = builder.build();
            long offset = callback.writeIndexBlock(completed);
            insert(new IndexBlock.Record(completed.startKey(), offset), level++);
            builder = new IndexBlock.Builder();
            indexBlockBuilders.set(level, builder);
        }

        builder.addRecord(record);
    }
}
