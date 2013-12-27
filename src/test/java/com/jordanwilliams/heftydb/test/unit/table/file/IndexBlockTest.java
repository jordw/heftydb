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

package com.jordanwilliams.heftydb.test.unit.table.file;


import com.jordanwilliams.heftydb.record.Key;
import com.jordanwilliams.heftydb.table.file.IndexBlock;
import com.jordanwilliams.heftydb.util.ByteBuffers;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class IndexBlockTest {

    private static final Key TEST_KEY_1 = new Key(ByteBuffers.fromString("An awesome test key"));
    private static final Key TEST_KEY_2 = new Key(ByteBuffers.fromString("Bad as I want to be"));
    private static final Key TEST_KEY_3 = new Key(ByteBuffers.fromString("Dog I am a test key"));
    private static final IndexBlock TEST_BLOCK;

    static {
        IndexBlock.Builder builder = new IndexBlock.Builder();
        builder.addRecord(new IndexBlock.Record(TEST_KEY_1, 0));
        builder.addRecord(new IndexBlock.Record(TEST_KEY_1, 5));
        builder.addRecord(new IndexBlock.Record(TEST_KEY_2, 1));
        builder.addRecord(new IndexBlock.Record(TEST_KEY_3, 2));
        builder.addRecord(new IndexBlock.Record(TEST_KEY_3, 3));

        TEST_BLOCK = builder.build();
    }

    @Test
    public void findRecordExactMatchTest() {
        List<Long> blockOffsets = TEST_BLOCK.blockOffsets(TEST_KEY_1);
        Assert.assertEquals("Offsets match", 0, blockOffsets.get(0).longValue());
    }

    @Test
    public void findRecordExactMatchEndTest() {
        List<Long> blockOffsets = TEST_BLOCK.blockOffsets(TEST_KEY_3);
        Assert.assertEquals("Offsets match", 2, blockOffsets.get(0).longValue());
        Assert.assertEquals("Offsets match", 3, blockOffsets.get(1).longValue());
    }

    @Test
    public void findRecordTest() {
        List<Long> blockOffsets = TEST_BLOCK.blockOffsets(new Key(ByteBuffers.fromString("Awesome")));
        Assert.assertEquals("Offsets match", 5, blockOffsets.get(0).longValue());
    }

    @Test
    public void findRecordMidTest() {
        List<Long> blockOffsets = TEST_BLOCK.blockOffsets(new Key(ByteBuffers.fromString("Box")));
        Assert.assertEquals("Offsets match", 1, blockOffsets.get(0).longValue());
    }

    @Test
    public void findRecordEndTest() {
        List<Long> blockOffsets = TEST_BLOCK.blockOffsets(new Key(ByteBuffers.fromString("Toast")));
        Assert.assertEquals("Offsets match", 3, blockOffsets.get(0).longValue());
    }
}
