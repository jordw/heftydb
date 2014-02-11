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

package com.jordanwilliams.heftydb.state;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.UniformReservoir;
import com.jordanwilliams.heftydb.db.Config;

import java.util.concurrent.TimeUnit;

public class Metrics {

    private static final String METRIC_PREFIX = "heftydb.";

    private final MetricRegistry metrics = new MetricRegistry();
    private final ConsoleReporter consoleReporter;

    public Metrics(Config config) {
        this.consoleReporter = ConsoleReporter.forRegistry(metrics).convertDurationsTo(TimeUnit.MILLISECONDS)
                .convertRatesTo(TimeUnit.SECONDS).build();
        consoleReporter.start(30, TimeUnit.SECONDS);

        initMetrics();
    }

    public Counter counter(String name){
        return metrics.counter(metricName(name));
    }

    public Histogram histogram(String name){
        return metrics.histogram(metricName(name));
    }

    public Timer timer(String name){
        return metrics.timer(metricName(name));
    }

    private void initMetrics(){
        //Main DB Metrics
        metrics.register(metricName("write"), new Timer());
        metrics.register(metricName("read"), new Timer());
        metrics.register(metricName("scan"), new Timer());

        //Write
        metrics.register(metricName("write.concurrentMemoryTableSerializers"), new Histogram(new UniformReservoir()));
        metrics.register(metricName("write.memoryTableSerialize"), new Timer());

        //Read
        metrics.register(metricName("read.tablesConsulted"), new Histogram(new UniformReservoir()));
    }

    private static String metricName(String name){
        return METRIC_PREFIX + name;
    }
}
