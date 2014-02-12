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

import com.codahale.metrics.Meter;
import com.codahale.metrics.RatioGauge;

public class CacheHitGauge extends RatioGauge {

    private final Meter hits = new Meter();
    private final Meter misses = new Meter();

    public void hit() {
        hits.mark();
    }

    public void miss() {
        misses.mark();
    }

    public void sample(boolean success) {
        if (success) {
            hit();
        } else {
            miss();
        }
    }

    @Override
    public Ratio getRatio() {
        return Ratio.of(hits.getFiveMinuteRate(), misses.getFiveMinuteRate() + hits.getFiveMinuteRate());
    }
}
