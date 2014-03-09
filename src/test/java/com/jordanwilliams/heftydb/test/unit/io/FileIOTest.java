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

package com.jordanwilliams.heftydb.test.unit.io;


import com.jordanwilliams.heftydb.io.AppendChannelFile;
import com.jordanwilliams.heftydb.io.AppendFile;
import com.jordanwilliams.heftydb.io.ImmutableChannelFile;
import com.jordanwilliams.heftydb.io.ImmutableFile;
import com.jordanwilliams.heftydb.test.base.FileTest;
import com.jordanwilliams.heftydb.test.helper.TestFileHelper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

public class FileIOTest extends FileTest {

    private static final ByteBuffer TEST_BYTES = ByteBuffer.wrap("I am some very impressive test data".getBytes());
    private static final ByteBuffer MORE_TEST_BYTES = ByteBuffer.wrap("Test data is very interesting".getBytes());

    private final Path testFile = TestFileHelper.TEMP_PATH.resolve("testfile");

    @Test
    public void bufferedAppendOffsetTest() throws IOException {
        AppendFile file = AppendChannelFile.open(testFile);

        long[] offsets = new long[100];

        for (int i = 0; i < 100; i++) {
            TEST_BYTES.rewind();
            offsets[i] = file.appendInt(TEST_BYTES.capacity());
            file.append(TEST_BYTES);
        }

        for (int i = 0; i < 100; i++) {
            long offset = offsets[i];
            Assert.assertEquals("Offset is correct", 0, offset % 39);
        }
    }

    @Test
    public void mutableDataFileTest() throws IOException {
        TEST_BYTES.rewind();
        MORE_TEST_BYTES.rewind();

        AppendFile file = AppendChannelFile.open(testFile);

        file.append(TEST_BYTES);
        file.append(MORE_TEST_BYTES);

        TEST_BYTES.rewind();
        file.append(TEST_BYTES);
        file.close();

        ByteBuffer readBuffer = ByteBuffer.allocate(TEST_BYTES.capacity());

        ImmutableFile readFile = ImmutableChannelFile.open(testFile);
        readFile.read(readBuffer, 0);

        TEST_BYTES.rewind();
        readBuffer.rewind();

        Assert.assertEquals("Read bytes", TEST_BYTES, readBuffer);
        Assert.assertEquals("File size", (TEST_BYTES.capacity() * 2) + MORE_TEST_BYTES.capacity(), readFile.size());
    }

    @Test
    public void mutableDataFilePrimitiveTest() throws IOException {
        AppendFile file = AppendChannelFile.open(testFile);
        file.appendInt(4);
        file.appendLong(8);
        file.close();

        ImmutableFile readFile = ImmutableChannelFile.open(testFile);
        Assert.assertEquals("Values match", 4, readFile.readInt(0));
        Assert.assertEquals("Values match", 8, readFile.readLong(4));
    }
}
