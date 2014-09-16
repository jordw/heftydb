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

/**
 * An implementation of MurmurHash3 based on https://code.google.com/p/smhasher/source/browse/trunk/MurmurHash3.cpp
 */
public class MurmurHash3 {

    private static long getblock(byte[] key, int i) {
        return (((long) key[i + 0] & 0x00000000000000FFL) << 0) | (((long) key[i + 1] & 0x00000000000000FFL) << 8) |
                (((long) key[i + 2] & 0x00000000000000FFL) << 16) | (((long) key[i + 3] & 0x00000000000000FFL) << 24)
                | (((long) key[i + 4] & 0x00000000000000FFL) << 32) | (((long) key[i + 5] & 0x00000000000000FFL) <<
                40) | (((long) key[i + 6] & 0x00000000000000FFL) << 48) | (((long) key[i + 7] & 0x00000000000000FFL)
                << 56);
    }

    private static long fmix(long k) {
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;

        return k;
    }

    public static long MurmurHash3_x64_64(final byte[] key) {
        long h1, h2, k1, k2, c1, c2;

        h1 = 0x9368e53c2f6af274L ^ 0;
        h2 = 0x586dcd208f7cd3fdL ^ 0;

        c1 = 0x87c37b91114253d5L;
        c2 = 0x4cf5ad432745937fL;

        for (int i = 0; i < key.length / 16; i++) {
            k1 = getblock(key, i * 2 * 8);
            k2 = getblock(key, (i * 2 + 1) * 8);

            k1 *= c1;
            k1 = (k1 << 23) | (k1 >>> 64 - 23);
            k1 *= c2;
            h1 ^= k1;
            h1 += h2;

            h2 = (h2 << 41) | (h2 >>> 64 - 41);

            k2 *= c2;
            k2 = (k2 << 23) | (k2 >>> 64 - 23);
            k2 *= c1;
            h2 ^= k2;
            h2 += h1;

            h1 = h1 * 3 + 0x52dce729;
            h2 = h2 * 3 + 0x38495ab5;

            c1 = c1 * 5 + 0x7b7d159c;
            c2 = c2 * 5 + 0x6bce6396;
        }

        k1 = 0;
        k2 = 0;

        int tail = (key.length >>> 4) << 4;

        switch (key.length & 15) {
            case 15:
                k2 ^= (long) key[tail + 14] << 48;
            case 14:
                k2 ^= (long) key[tail + 13] << 40;
            case 13:
                k2 ^= (long) key[tail + 12] << 32;
            case 12:
                k2 ^= (long) key[tail + 11] << 24;
            case 11:
                k2 ^= (long) key[tail + 10] << 16;
            case 10:
                k2 ^= (long) key[tail + 9] << 8;
            case 9:
                k2 ^= (long) key[tail + 8] << 0;

            case 8:
                k1 ^= (long) key[tail + 7] << 56;
            case 7:
                k1 ^= (long) key[tail + 6] << 48;
            case 6:
                k1 ^= (long) key[tail + 5] << 40;
            case 5:
                k1 ^= (long) key[tail + 4] << 32;
            case 4:
                k1 ^= (long) key[tail + 3] << 24;
            case 3:
                k1 ^= (long) key[tail + 2] << 16;
            case 2:
                k1 ^= (long) key[tail + 1] << 8;
            case 1:
                k1 ^= (long) key[tail + 0] << 0;
                k1 *= c1;
                k1 = (k1 << 23) | (k1 >>> 64 - 23);
                k1 *= c2;
                h1 ^= k1;
                h1 += h2;

                h2 = (h2 << 41) | (h2 >>> 64 - 41);

                k2 *= c2;
                k2 = (k2 << 23) | (k2 >>> 64 - 23);
                k2 *= c1;
                h2 ^= k2;
                h2 += h1;

                h1 = h1 * 3 + 0x52dce729;
                h2 = h2 * 3 + 0x38495ab5;

                c1 = c1 * 5 + 0x7b7d159c;
                c2 = c2 * 5 + 0x6bce6396;
        }

        h2 ^= key.length;

        h1 += h2;
        h2 += h1;

        h1 = fmix(h1);
        h2 = fmix(h2);

        h1 += h2;

        return h1;
    }
}
