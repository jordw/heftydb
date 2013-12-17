package com.jordanwilliams.heftydb.test.unit.offheap;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class Allocator {

    static final Unsafe unsafe;

    public static long allocate(long size) {
        return unsafe.allocateMemory(size);
    }

    public static void free(long address) {
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
