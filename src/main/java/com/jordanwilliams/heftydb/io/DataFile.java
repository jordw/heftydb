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


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

public interface DataFile {

    public long append(ByteBuffer bufferToWrite) throws IOException;

    public long appendInt(int intToWrite) throws IOException;

    public long appendLong(long longToWrite) throws IOException;

    public long read(ByteBuffer bufferToRead, long position) throws IOException;

    public int readInt(long position) throws IOException;

    public long readLong(long position) throws IOException;

    public long write(ByteBuffer bufferToWrite, long position) throws IOException;

    public long writeLong(long longToWrite, long position) throws IOException;

    public long writeInt(int intToWrite, long position) throws IOException;

    public long size() throws IOException;

    public void sync() throws IOException;

    public void close() throws IOException;

    public Path path();
}
