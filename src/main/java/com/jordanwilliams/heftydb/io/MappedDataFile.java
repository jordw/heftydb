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

package com.jordanwilliams.heftydb.io;

import net.jcip.annotations.ThreadSafe;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

@ThreadSafe
public class MappedDataFile implements DataFile {

    private final Path path;
    private final MappedByteBuffer mappedBuffer;
    private final FileChannel channel;

    private MappedDataFile(Path path, FileChannel fileChannel, MappedByteBuffer mappedBuffer) {
        this.channel = fileChannel;
        this.path = path;
        this.mappedBuffer = mappedBuffer;
    }

    @Override
    public long append(ByteBuffer bufferToWrite) {
        throw new UnsupportedOperationException("MappedDataFiles are read only");
    }

    @Override
    public long appendInt(int intToWrite) throws IOException {
        throw new UnsupportedOperationException("MappedDataFiles are read only");
    }

    @Override
    public long appendLong(long longToWrite) throws IOException {
        throw new UnsupportedOperationException("MappedDataFiles are read only");
    }

    @Override
    public long read(ByteBuffer bufferToRead, long position) {
        ByteBuffer duplicateBuffer = mappedBuffer.duplicate();
        duplicateBuffer.position((int) position);

        for (int i = 0; i < bufferToRead.capacity(); i++) {
            bufferToRead.put(duplicateBuffer.get());
        }

        return bufferToRead.capacity();
    }

    @Override
    public int readInt(long position) throws IOException {
        ByteBuffer duplicateBuffer = mappedBuffer.duplicate();
        duplicateBuffer.position((int) position);
        int value = duplicateBuffer.getInt();
        return value;
    }

    @Override
    public long readLong(long position) throws IOException {
        ByteBuffer duplicateBuffer = mappedBuffer.duplicate();
        duplicateBuffer.position((int) position);
        long value = duplicateBuffer.getLong();
        return value;
    }

    @Override
    public long write(ByteBuffer bufferToWrite, long position) {
        throw new UnsupportedOperationException("MappedDataFiles are read only");
    }

    @Override
    public long writeLong(long longToWrite, long position) throws IOException {
        throw new UnsupportedOperationException("MappedDataFiles are read only");
    }

    @Override
    public long writeInt(int intToWrite, long position) throws IOException {
        throw new UnsupportedOperationException("MappedDataFiles are read only");
    }

    @Override
    public long size() throws IOException {
        return mappedBuffer.capacity();
    }

    @Override
    public void sync() {
        throw new UnsupportedOperationException("MappedDataFiles are read only");
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    @Override
    public Path path() {
        return path;
    }

    public static MappedDataFile open(Path path) throws IOException {
        FileChannel dataFileChannel = FileChannel.open(path, StandardOpenOption.READ);
        MappedByteBuffer mappedByteBuffer = dataFileChannel.map(FileChannel.MapMode.READ_ONLY, 0,
                dataFileChannel.size());
        return new MappedDataFile(path, dataFileChannel, mappedByteBuffer);
    }
}
