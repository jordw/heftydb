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

import com.jordanwilliams.heftydb.events.DataFileEvents;
import net.jcip.annotations.ThreadSafe;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;

@ThreadSafe
public class MutableDataFile implements DataFile {

    private static final DataFileEvents events = new DataFileEvents("File IO");

    private final Path path;
    private final FileChannel channel;
    private final AtomicLong position = new AtomicLong();

    private MutableDataFile(Path path) throws IOException {
        this.path = path;
        this.channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
    }

    @Override
    public long append(ByteBuffer bufferToWrite) throws IOException {
        long writtenPosition = position.getAndAdd(bufferToWrite.capacity());
        write(bufferToWrite, writtenPosition);
        return writtenPosition;
    }

    @Override
    public long read(ByteBuffer bufferToRead, long position) throws IOException {
        events.startRead();
        long bytesRead = channel.read(bufferToRead, position);
        events.finishRead();
        return bytesRead;
    }

    @Override
    public long write(ByteBuffer bufferToWrite, long position) throws IOException {
        events.startWrite();
        bufferToWrite.rewind();
        long bytesWritten = channel.write(bufferToWrite, position);
        events.finishWrite();
        return bytesWritten;
    }

    @Override
    public long size() throws IOException {
        return channel.size();
    }

    @Override
    public void sync() throws IOException {
        events.startSync();
        channel.force(true);
        events.finishSync();
    }

    @Override
    public void rename(String newName) throws IOException {
        Files.move(path, path.resolveSibling(newName), StandardCopyOption.ATOMIC_MOVE);
    }

    @Override
    public void delete() throws IOException {
        Files.deleteIfExists(path);
    }

    @Override
    public void close() throws IOException {
        sync();
        channel.close();
    }

    @Override
    public Path path() {
        return path;
    }

    public static MutableDataFile open(Path path) throws IOException {
        return new MutableDataFile(path);
    }

    public static DataFileEvents events() {
        return events;
    }
}
