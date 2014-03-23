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

package com.jordanwilliams.heftydb.util;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Some utilities for working with ByteBuffers.
 */
public class ByteBuffers {

    public static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    public static ByteBuffer fromString(String string) {
        return ByteBuffer.wrap(string.getBytes(Charset.defaultCharset()));
    }

    public static String toString(ByteBuffer byteBuffer) {
        byte[] bytes = new byte[byteBuffer.capacity()];

        for (int i = 0; i < byteBuffer.capacity(); i++) {
            bytes[i] = byteBuffer.get(i);
        }

        return new String(bytes);
    }
}
