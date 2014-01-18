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

package com.jordanwilliams.heftydb.test.unit.offheap;

import com.jordanwilliams.heftydb.offheap.Memory;

import org.junit.Assert;
import org.junit.Test;

public class MemoryTest {

  @Test
  public void retainReleaseTest() {
    Memory memory = Memory.allocate(256);

    memory.retain();
    memory.retain();

    memory.release();
    memory.release();
    memory.release();

    Assert.assertTrue("Memory has been freed", memory.isFree());
  }
}
