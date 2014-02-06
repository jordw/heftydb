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

package com.jordanwilliams.heftydb.events;

import com.jordanwilliams.heftydb.metrics.CounterMetric;
import com.jordanwilliams.heftydb.metrics.HistogramMetric;
import com.jordanwilliams.heftydb.metrics.MetricsCollection;
import com.jordanwilliams.heftydb.metrics.StopWatch;

public class MemoryEvents implements Events {

    private static final ThreadLocal<StopWatch> timer = new ThreadLocal<StopWatch>();

    private final MetricsCollection metrics;
    private final CounterMetric allocatedMemory = new CounterMetric("memoryAllocated");
    private final HistogramMetric mallocTime = new HistogramMetric("mallocTime", "us");
    private final HistogramMetric freeTime = new HistogramMetric("freeTime", "us");

    public MemoryEvents() {
        metrics = new MetricsCollection("Off heap Memory");
        metrics.put(allocatedMemory);
        metrics.put(mallocTime);
        metrics.put(freeTime);
    }

    public void startMalloc(long bytes){
        timer.set(StopWatch.start());
        allocatedMemory.add(bytes);
    }

    public void finishMalloc() {
        mallocTime.record(timer.get().elapsedMicros());
        timer.remove();
    }

    public void startFree(long bytes){
        timer.set(StopWatch.start());
        allocatedMemory.subtract(bytes);
    }

    public void finishFree() {
        freeTime.record(timer.get().elapsedMicros());
        timer.remove();
    }

    @Override
    public String summary() {
        return metrics.summary();
    }

    @Override
    public MetricsCollection metrics() {
        return metrics;
    }
}
