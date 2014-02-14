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

package com.jordanwilliams.heftydb.test.unit.util;

import com.jordanwilliams.heftydb.util.XORShiftRandom;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class XORShiftRandomTest {


    @Test
    public void randomTest() {
        XORShiftRandom random = new XORShiftRandom(32);
        List<Integer> generatedNumbers = new ArrayList<Integer>();

        for (int i = 0; i < 128; i++) {
            generatedNumbers.add(random.nextInt(1024));
        }

        random = new XORShiftRandom(32);

        for (int number : generatedNumbers) {
            Assert.assertEquals("Generated numbers are equal", number, random.nextInt(1024));
        }
    }
}
