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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@ThreadSafe
public class MetricsCollection {

  private final Map<String, Metric> metrics = new ConcurrentHashMap<String, Metric>();
  private final String name;

  public MetricsCollection(String name) {
    this.name = name;
  }

  public MetricsCollection() {
    this("");
  }

  public void put(Metric metric) {
    metrics.put(metric.name(), metric);
  }

  public <T extends Metric> T get(String name) {
    return (T) metrics.get(name);
  }

  public String summary() {
    StringBuilder str = new StringBuilder();

    if (name != null && !name.isEmpty()) {
      str.append(name + "\n");
    }

    for (String key : metrics.keySet()) {
      String summary = metrics.get(key).summary();

      if (summary != null && !summary.isEmpty()) {
        str.append("    " + summary + "\n");
      }
    }

    return str.toString();
  }
}
