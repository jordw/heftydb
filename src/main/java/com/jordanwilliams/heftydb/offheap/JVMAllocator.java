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

package com.jordanwilliams.heftydb.offheap;


import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class JVMAllocator implements Allocator {

    static final Unsafe unsafe;

    @Override
    public long allocate(long size) {
        return unsafe.allocateMemory(size);
    }

    @Override
    public void free(long address) {
        unsafe.freeMemory(address);
    }

    static {
        try {
            Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (sun.misc.Unsafe) field.get(null);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
