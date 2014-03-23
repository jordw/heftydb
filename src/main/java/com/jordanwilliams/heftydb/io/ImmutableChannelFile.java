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

package com.jordanwilliams.heftydb.io;

import com.jordanwilliams.heftydb.util.Sizes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * A read only wrapper around a FileChannel
 */
public class ImmutableChannelFile implements ImmutableFile {

    private static final ThreadLocal<ByteBuffer> primitiveBuffer = new ThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() {
            ByteBuffer primitiveBuffer = ByteBuffer.allocate(Sizes.LONG_SIZE);
            return primitiveBuffer;
        }
    };

    private final FileChannel channel;

    public ImmutableChannelFile(FileChannel channel) {
        this.channel = channel;
    }

    @Override
    public long read(ByteBuffer bufferToRead, long position) throws IOException {
        return channel.read(bufferToRead, position);
    }

    @Override
    public int readInt(long position) throws IOException {
        ByteBuffer intBuffer = intBuffer();
        channel.read(intBuffer, position);
        intBuffer.rewind();
        return intBuffer.getInt();
    }

    @Override
    public long readLong(long position) throws IOException {
        ByteBuffer longBuffer = longBuffer();
        channel.read(longBuffer, position);
        longBuffer.rewind();
        return longBuffer.getLong();
    }

    @Override
    public long size() throws IOException {
        return channel.size();
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    private ByteBuffer intBuffer() {
        ByteBuffer buffer = primitiveBuffer.get();
        buffer.rewind();
        buffer.limit(Sizes.INT_SIZE);
        return buffer;
    }

    private ByteBuffer longBuffer() {
        ByteBuffer buffer = primitiveBuffer.get();
        buffer.rewind();
        buffer.limit(Sizes.LONG_SIZE);
        return buffer;
    }

    public static ImmutableFile open(Path path) throws IOException {
        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
        return new ImmutableChannelFile(channel);
    }
}
