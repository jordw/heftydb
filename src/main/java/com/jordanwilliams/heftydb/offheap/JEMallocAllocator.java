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

package com.jordanwilliams.heftydb.offheap;

import com.sun.jna.Library;
import com.sun.jna.Native;

public class JEMallocAllocator implements Allocator {

    private interface JEMalloc extends Library {

        long malloc(long size);

        void free(long address);
    }

    private final JEMalloc library;

    public JEMallocAllocator() {
        library = (JEMalloc) Native.loadLibrary("jemalloc", JEMalloc.class);
    }

    @Override
    public long allocate(long bytes) {
        return library.malloc(bytes);
    }

    @Override
    public void free(long address) {
        library.free(address);
    }
}
