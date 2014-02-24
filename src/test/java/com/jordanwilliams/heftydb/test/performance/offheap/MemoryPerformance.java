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

package com.jordanwilliams.heftydb.test.performance.offheap;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.jordanwilliams.heftydb.offheap.MemoryPointer;
import com.jordanwilliams.heftydb.test.helper.PerformanceHelper;

import java.util.Random;

public class MemoryPerformance {

    public static void main(String[] args) throws Exception {
        MetricRegistry metrics = new MetricRegistry();
        ConsoleReporter reporter = PerformanceHelper.consoleReporter(metrics);
        Timer timer = metrics.timer("allocations");

        Random random = new Random(System.nanoTime());
        int iterations = 1000000;
        MemoryPointer[] pointerArray = new MemoryPointer[iterations];

        for (int i = 0; i < pointerArray.length; i++) {
            Timer.Context watch = timer.time();
            pointerArray[i] = MemoryPointer.allocate(random.nextInt(16384));
            watch.stop();
        }

        reporter.report();
    }

}
