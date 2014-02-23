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


import com.jordanwilliams.heftydb.io.ChannelDataFile;
import com.jordanwilliams.heftydb.test.base.FileTest;
import com.jordanwilliams.heftydb.test.helper.TestFileHelper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

public class DataFileTest extends FileTest {

    private static final ByteBuffer TEST_BYTES = ByteBuffer.wrap("I am some very impressive test data".getBytes());
    private static final ByteBuffer MORE_TEST_BYTES = ByteBuffer.wrap("Test data is very interesting".getBytes());

    private final Path testFile = TestFileHelper.TEMP_PATH.resolve("testfile");

    @Test
    public void bufferedAppendOffsetTest() throws IOException {
        ChannelDataFile file = ChannelDataFile.open(testFile);

        long[] offsets = new long[100];

        for (int i = 0; i < 100; i++){
            TEST_BYTES.rewind();
            offsets[i] = file.appendInt(TEST_BYTES.capacity());
            file.append(TEST_BYTES);
        }

        for (int i = 0; i < 100; i++){
            long offset = offsets[i];
            Assert.assertEquals("Offset is correct", 0, offset % 39);
        }
    }

    @Test
    public void mutableDataFileTest() throws IOException {
        TEST_BYTES.rewind();
        MORE_TEST_BYTES.rewind();

        ChannelDataFile file = ChannelDataFile.open(testFile);

        file.append(TEST_BYTES);
        file.append(MORE_TEST_BYTES);

        TEST_BYTES.rewind();
        file.write(TEST_BYTES, file.size());

        ByteBuffer readBuffer = ByteBuffer.allocate(TEST_BYTES.capacity());
        file.read(readBuffer, 0);

        TEST_BYTES.rewind();
        readBuffer.rewind();

        Assert.assertEquals("Read bytes", TEST_BYTES, readBuffer);
        Assert.assertEquals("File size", (TEST_BYTES.capacity() * 2) + MORE_TEST_BYTES.capacity(), file.size());
    }

    @Test
    public void mutableDataFilePrimitiveTest() throws IOException {
        ChannelDataFile file = ChannelDataFile.open(testFile);

        file.appendInt(4);
        file.appendLong(8);
        file.writeInt(4, file.size());
        file.writeLong(8, file.size());

        Assert.assertEquals("Values match", 4, file.readInt(0));
        Assert.assertEquals("Values match", 8, file.readLong(4));
        Assert.assertEquals("Values match", 4, file.readInt(12));
        Assert.assertEquals("Values match", 8, file.readLong(16));
    }
}
