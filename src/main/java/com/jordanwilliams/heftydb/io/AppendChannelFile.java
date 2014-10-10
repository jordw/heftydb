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

import com.jordanwilliams.heftydb.offheap.JVMUnsafe;
import com.jordanwilliams.heftydb.offheap.MemoryAllocator;
import com.jordanwilliams.heftydb.offheap.MemoryPointer;
import com.jordanwilliams.heftydb.util.Sizes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A buffered, append only FileChannel wrapper class.
 */
public class AppendChannelFile implements AppendFile {

    private static final int APPEND_BUFFER_SIZE = JVMUnsafe.unsafe.pageSize();

    private static final ThreadLocal<ByteBuffer> primitiveBuffer = new ThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() {
            return ByteBuffer.allocate(Sizes.LONG_SIZE);
        }
    };

    private final MemoryPointer appendBuffer;
    private final FileChannel channel;
    private final AtomicLong appendPosition = new AtomicLong();

    private AppendChannelFile(FileChannel channel, int appendBufferSize) {
        this.channel = channel;
        this.appendBuffer = MemoryAllocator.allocate(appendBufferSize);
    }

    @Override
    public synchronized long append(ByteBuffer bufferToWrite) throws IOException {
        int writeLength = bufferToWrite.limit() - bufferToWrite.position();
        long writtenPosition = appendPosition.getAndAdd(writeLength);

        if (writeLength > APPEND_BUFFER_SIZE) {
            flushAppendBuffer();
            channel.write(bufferToWrite);
        } else {
            ByteBuffer buffer = appendBuffer.directBuffer();

            while (bufferToWrite.position() < bufferToWrite.limit()) {
                if (buffer.position() == buffer.limit()) {
                    flushAppendBuffer();
                }

                buffer.put(bufferToWrite.get());
            }
        }

        return writtenPosition;
    }

    @Override
    public long appendInt(int intToWrite) throws IOException {
        return append(intBuffer(intToWrite));
    }

    @Override
    public long appendLong(long longToWrite) throws IOException {
        return append(longBuffer(longToWrite));
    }

    @Override
    public synchronized long size() throws IOException {
        flushAppendBuffer();
        return channel.size();
    }

    @Override
    public synchronized void sync() throws IOException {
        flushAppendBuffer();
        channel.force(true);
    }

    @Override
    public synchronized void close() throws IOException {
        if (channel.isOpen()) {
            flushAppendBuffer();
            sync();

            if (appendBuffer != null && !appendBuffer.isFree()) {
                appendBuffer.free();
            }

            channel.close();
        }
    }

    private ByteBuffer intBuffer() {
        ByteBuffer buffer = primitiveBuffer.get();
        buffer.rewind();
        buffer.limit(Sizes.INT_SIZE);
        return buffer;
    }

    private ByteBuffer intBuffer(int value) {
        ByteBuffer intBuffer = intBuffer();
        intBuffer.putInt(value);
        intBuffer.rewind();
        return intBuffer;
    }

    private ByteBuffer longBuffer() {
        ByteBuffer buffer = primitiveBuffer.get();
        buffer.rewind();
        buffer.limit(Sizes.LONG_SIZE);
        return buffer;
    }

    private ByteBuffer longBuffer(long value) {
        ByteBuffer longBuffer = longBuffer();
        longBuffer.putLong(value);
        longBuffer.rewind();
        return longBuffer;
    }

    private void flushAppendBuffer() throws IOException {
        ByteBuffer buffer = appendBuffer.directBuffer();

        if (buffer.position() == 0) {
            return;
        }

        buffer.limit(buffer.position());
        buffer.rewind();
        channel.write(buffer);
        buffer.limit(buffer.capacity());
        buffer.rewind();
    }

    public static AppendFile open(Path path) throws IOException {
        FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        return new AppendChannelFile(channel, APPEND_BUFFER_SIZE);
    }
}
