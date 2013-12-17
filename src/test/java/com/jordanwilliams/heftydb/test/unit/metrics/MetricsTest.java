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

package com.jordanwilliams.heftydb.test.unit.metrics;

import com.jordanwilliams.heftydb.metrics.HistogramMetric;
import com.jordanwilliams.heftydb.metrics.RateMetric;
import com.jordanwilliams.heftydb.metrics.CounterMetric;
import org.junit.Assert;
import org.junit.Test;

public class MetricsTest {

    @Test
    public void rateMetricTest() {
        RateMetric rateTest = new RateMetric("testRateMetric");
        rateTest.record(RateMetric.Sample.SUCCESS);
        rateTest.record(RateMetric.Sample.FAILURE);

        Assert.assertEquals("Rate", 0.5, rateTest.rate(), 0);
    }

    @Test
    public void histogramMetricTest() {
        HistogramMetric numericTest = new HistogramMetric("testNumericMetric");

        for (int i = 0; i < 100; i++) {
            numericTest.record(i);
        }

        Assert.assertEquals("Average", 49.5, numericTest.avg(), 0);
        Assert.assertEquals("P50", 50, numericTest.percentile(50), 0);
        Assert.assertEquals("P90", 90, numericTest.percentile(90), 0);
    }

    @Test
    public void counterMetricTest() {
        CounterMetric counterTest = new CounterMetric("testCounterMetric");
        counterTest.increment();
        counterTest.decrement();
        counterTest.add(10);
        counterTest.subtract(9);

        Assert.assertEquals("Counter", 1, counterTest.value());
    }
}
