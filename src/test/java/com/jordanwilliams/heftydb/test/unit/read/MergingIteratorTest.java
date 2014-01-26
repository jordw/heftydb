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

package com.jordanwilliams.heftydb.test.unit.read;

import com.google.common.primitives.Ints;
import com.jordanwilliams.heftydb.read.MergingIterator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;

public class MergingIteratorTest {

    private static final int[] ARRAY1 = {1, 2, 3, 4, 5, 6};
    private static final int[] ARRAY2 = {1, 4, 5, 7, 9, 10};
    private static final int[] MERGED_ARRAY = {1, 1, 2, 3, 4, 4, 5, 5, 6, 7, 9, 10};

    @Test
    public void mergeTest() {
        MergingIterator<Integer> mergingIterator = new MergingIterator<Integer>(Ints.asList(ARRAY1).iterator(), Ints.asList(ARRAY2).iterator());

        Iterator<Integer> mergedIterator = Ints.asList(MERGED_ARRAY).iterator();

        while (mergedIterator.hasNext()) {
            Assert.assertEquals("Merged values match", mergedIterator.next(), mergingIterator.next());
        }
    }
}
