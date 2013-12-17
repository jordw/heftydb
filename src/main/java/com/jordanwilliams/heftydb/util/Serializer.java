package com.jordanwilliams.heftydb.util;


import com.jordanwilliams.heftydb.test.unit.offheap.Memory;

import java.nio.ByteBuffer;

public interface Serializer<I, O> {

    public O serialize(I data);

    public I deserialize(O in);

    public long serializedSize(I data);

    public interface OffHeapSerializer<T> extends Serializer<T, Memory> {

    }

    public interface ByteBufferSerializer<T> extends Serializer<T, ByteBuffer> {

    }
}
