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

package com.jordanwilliams.heftydb.test.unit.offheap;

import com.jordanwilliams.heftydb.offheap.BitSet;
import com.jordanwilliams.heftydb.util.Sizes;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class BitSetTest {

    @Test
    public void getSetTest() {
        BitSet.Builder testSetBuilder = new BitSet.Builder(256, Sizes.INT_SIZE);
        boolean[] values = new boolean[256];

        Random random = new Random(System.nanoTime());

        for (int i = 0; i < 256; i++) {
            boolean nextValue = random.nextBoolean();
            testSetBuilder.set(i, nextValue);
            values[i] = nextValue;
        }

        BitSet testSet = testSetBuilder.build();

        for (int i = 0; i < 256; i++) {
            Assert.assertEquals("Values match", values[i], testSet.get(i));
        }
    }
}
