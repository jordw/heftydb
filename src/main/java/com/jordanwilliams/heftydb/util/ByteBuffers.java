package com.jordanwilliams.heftydb.util;

import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

public class ByteBuffers {

    public static ByteBuffer fromString(String string){
        return ByteBuffer.wrap(string.getBytes());
    }

    public static void free(ByteBuffer bb) {
        if (bb == null){
            return;
        }

        Cleaner cleaner = ((DirectBuffer) bb).cleaner();

        if (cleaner != null){
            cleaner.clean();
        }
    }
}
