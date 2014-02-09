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

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.jordanwilliams.heftydb.state.Config;

import java.util.concurrent.TimeUnit;

public class Metrics {

    private final MetricRegistry metrics = new MetricRegistry();
    private final JmxReporter jmxReporter;

    public Metrics(Config config) {
        this.jmxReporter = JmxReporter.forRegistry(metrics).convertDurationsTo(TimeUnit.MILLISECONDS).convertRatesTo
                (TimeUnit.SECONDS).inDomain(config.tableDirectory().toString()).build();
        jmxReporter.start();
    }

    public void register(String name, Metric metric) {
        metrics.register(name, metric);
    }
}
