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

package com.jordanwilliams.heftydb.metrics;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.UniformReservoir;
import com.jordanwilliams.heftydb.db.Config;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class Metrics {

    private static final String METRIC_PREFIX = "heftydb.";

    private final Map<String, CacheHitGauge> gaugeCache = new ConcurrentHashMap<String, CacheHitGauge>();
    private final MetricRegistry metrics = new MetricRegistry();
    private final ConsoleReporter consoleReporter;

    public Metrics(Config config) {
        this.consoleReporter = ConsoleReporter.forRegistry(metrics).convertDurationsTo(TimeUnit.MILLISECONDS)
                .convertRatesTo(TimeUnit.SECONDS).build();

        if (config.printMetrics()) {
            consoleReporter.start(30, TimeUnit.SECONDS);
        }

        initMetrics();
    }

    public Counter counter(String name) {
        return metrics.counter(metricName(name));
    }

    public Meter meter(String name) {
        return metrics.meter(metricName(name));
    }

    public CacheHitGauge hitGauge(String name) {
        CacheHitGauge cacheHitGauge = gaugeCache.get(metricName(name));

        if (cacheHitGauge == null) {
            cacheHitGauge = (CacheHitGauge) metrics.getGauges().get(metricName(name));
            gaugeCache.put(metricName(name), cacheHitGauge);
        }

        return cacheHitGauge;
    }

    public void gauge(String name, Gauge<?> gauge) {
        metrics.register(metricName(name), gauge);
    }

    public Histogram histogram(String name) {
        return metrics.histogram(metricName(name));
    }

    public Timer timer(String name) {
        return metrics.timer(metricName(name));
    }

    private void initMetrics() {
        //Main DB Metrics
        metrics.register(metricName("write"), new Timer(new UniformReservoir()));
        metrics.register(metricName("write.rate"), new Meter());
        metrics.register(metricName("read"), new Timer(new UniformReservoir()));
        metrics.register(metricName("read.rate"), new Meter());
        metrics.register(metricName("scan"), new Timer(new UniformReservoir()));
        metrics.register(metricName("scan.rate"), new Meter());

        //Write
        metrics.register(metricName("write.concurrentMemoryTableSerializers"), new Histogram(new UniformReservoir()));
        metrics.register(metricName("write.memoryTableSerialize"), new Timer(new UniformReservoir()));

        //Read
        metrics.register(metricName("read.tablesConsulted"), new Histogram(new UniformReservoir()));
        metrics.register(metricName("read.bloomFilterFalsePositiveRate"), new CacheHitGauge());
        metrics.register(metricName("read.recordNotFoundRate"), new CacheHitGauge());

        //FileTable
        metrics.register(metricName("table.cacheHitRate"), new CacheHitGauge());

        //Index
        metrics.register(metricName("index.searchLevels"), new Histogram(new UniformReservoir()));
        metrics.register(metricName("index.cacheHitRate"), new CacheHitGauge());

        //Compactor
        metrics.register(metricName("compactor.concurrentTasks"), new Histogram(new UniformReservoir()));
        metrics.register(metricName("compactor.taskExecution"), new Timer(new UniformReservoir()));
    }

    private static String metricName(String name) {
        return METRIC_PREFIX + name;
    }
}
