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

package com.jordanwilliams.hefty.metrics;

import com.jordanwilliams.heftydb.metrics.Metric;
import net.jcip.annotations.ThreadSafe;

import java.util.concurrent.atomic.AtomicLong;

@ThreadSafe
public class CounterMetric implements Metric {

    private final String name;
    private final AtomicLong count = new AtomicLong();

    public CounterMetric(String name) {
        this.name = name;
    }

    public long value() {
        return count.longValue();
    }

    public void increment() {
        count.incrementAndGet();
    }

    public void decrement() {
        count.decrementAndGet();
    }

    public void add(long delta) {
        count.addAndGet(delta);
    }

    public void subtract(long delta) {
        count.addAndGet(delta * -1);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String summary() {
        StringBuilder str = new StringBuilder();
        str.append(name + " ");

        str.append("value:" + value());

        return str.toString();
    }
}
