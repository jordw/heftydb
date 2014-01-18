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

package com.jordanwilliams.heftydb.metrics;

import net.jcip.annotations.ThreadSafe;

import java.util.concurrent.atomic.AtomicLong;

@ThreadSafe
public class RateMetric implements Metric {

  public enum Sample {
    SUCCESS, FAILURE
  }

  private final String name;
  private final AtomicLong successCount = new AtomicLong(), failureCount = new AtomicLong();

  public RateMetric(String name) {
    this.name = name;
  }

  public void record(Sample sample) {
    if (sample.equals(Sample.SUCCESS)) {
      successCount.incrementAndGet();
    } else {
      failureCount.incrementAndGet();
    }
  }

  public double rate() {
    return successCount.doubleValue() / (successCount.doubleValue() + failureCount.doubleValue());
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public String summary() {
    StringBuilder str = new StringBuilder();
    str.append(name + " ");
    str.append("rate:" + (rate() * 100) + "%");
    return str.toString();
  }
}
