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

package com.jordanwilliams.heftydb.events;


import com.jordanwilliams.heftydb.metrics.HistogramMetric;
import com.jordanwilliams.heftydb.metrics.MetricsCollection;
import com.jordanwilliams.heftydb.metrics.StopWatch;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class DataFileEvents implements Events {

    private static final ThreadLocal<StopWatch> timer = new ThreadLocal<StopWatch>();

    private final MetricsCollection metrics;
    private final HistogramMetric readTime = new HistogramMetric("readTime", "us");
    private final HistogramMetric writeTime = new HistogramMetric("writeTime", "us");
    private final HistogramMetric syncTime = new HistogramMetric("syncTime", "us");

    public DataFileEvents(String type) {
        metrics = new MetricsCollection(type);
        metrics.put(readTime);
        metrics.put(writeTime);
        metrics.put(syncTime);
    }

    public void startRead() {
        timer.set(StopWatch.start());
    }

    public void finishRead() {
        readTime.record(timer.get().elapsedMicros());
        timer.remove();
    }

    public void startWrite() {
        timer.set(StopWatch.start());
    }

    public void finishWrite() {
        writeTime.record(timer.get().elapsedMicros());
        timer.remove();
    }

    public void startSync() {
        timer.set(StopWatch.start());
    }

    public void finishSync() {
        syncTime.record(timer.get().elapsedMicros());
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
