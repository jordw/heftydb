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
import java.util.concurrent.atomic.AtomicLong;

public class ChannelDataFile implements DataFile {

    private static final int APPEND_BUFFER_SIZE = 8192;

    private static final ThreadLocal<ByteBuffer> primitiveBuffer = new ThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() {
            ByteBuffer primitiveBuffer = ByteBuffer.allocate(Sizes.LONG_SIZE);
            return primitiveBuffer;
        }
    };

    private final ThreadLocal<ByteBuffer> appendBuffer = new ThreadLocal<ByteBuffer>(){
        @Override
        protected ByteBuffer initialValue(){
            return ByteBuffer.allocateDirect(APPEND_BUFFER_SIZE);
        }
    };

    private final Path path;
    private final FileChannel channel;
    private final AtomicLong appendPosition = new AtomicLong();

    private ChannelDataFile(Path path, FileChannel fileChannel) {
        this.path = path;
        this.channel = fileChannel;
    }

    @Override
    public long append(ByteBuffer bufferToWrite) throws IOException {
        int writeLength = bufferToWrite.limit() - bufferToWrite.position();
        long writtenPosition = appendPosition.getAndAdd(writeLength);

        if (writeLength > APPEND_BUFFER_SIZE){
            flushAppendBuffer();
            channel.write(bufferToWrite);
        } else {
            ByteBuffer buffer = appendBuffer.get();

            while (bufferToWrite.position() < bufferToWrite.limit()){
                if (buffer.position() == buffer.limit()){
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
    public long read(ByteBuffer bufferToRead, long position) throws IOException {
        flushAppendBuffer();
        long bytesRead = channel.read(bufferToRead, position);
        return bytesRead;
    }

    @Override
    public int readInt(long position) throws IOException {
        flushAppendBuffer();
        ByteBuffer intBuffer = intBuffer();
        channel.read(intBuffer, position);
        intBuffer.rewind();
        return intBuffer.getInt();
    }

    @Override
    public long readLong(long position) throws IOException {
        flushAppendBuffer();
        ByteBuffer longBuffer = longBuffer();
        channel.read(longBuffer, position);
        longBuffer.rewind();
        return longBuffer.getLong();
    }

    @Override
    public long write(ByteBuffer bufferToWrite, long position) throws IOException {
        flushAppendBuffer();
        bufferToWrite.rewind();
        long bytesWritten = channel.write(bufferToWrite, position);
        return bytesWritten;
    }

    @Override
    public long writeLong(long longToWrite, long position) throws IOException {
        return write(longBuffer(longToWrite), position);
    }

    @Override
    public long writeInt(int intToWrite, long position) throws IOException {
        return write(intBuffer(intToWrite), position);
    }

    @Override
    public long size() throws IOException {
        flushAppendBuffer();
        return channel.size();
    }

    @Override
    public void sync() throws IOException {
        flushAppendBuffer();
        channel.force(true);
    }

    @Override
    public synchronized void close() throws IOException {
        if (channel.isOpen()) {
            sync();
            channel.close();
        }
    }

    @Override
    public Path path() {
        return path;
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
        return intBuffer();
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
        ByteBuffer buffer = appendBuffer.get();

        if (buffer.position() == 0){
            return;
        }

        buffer.limit(buffer.position());
        buffer.rewind();
        channel.write(buffer);
        buffer.limit(buffer.capacity());
        buffer.rewind();
    }

    public static ChannelDataFile open(Path path) throws IOException {
        FileChannel dataFileChannel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE,
                StandardOpenOption.CREATE);
        return new ChannelDataFile(path, dataFileChannel);
    }
}
