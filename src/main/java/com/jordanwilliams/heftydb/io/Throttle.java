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

package com.jordanwilliams.heftydb.io;

import org.isomorphism.util.TokenBucket;
import org.isomorphism.util.TokenBuckets;

import java.util.concurrent.TimeUnit;

public class Throttle {

    public static Throttle MAX = new Throttle(Integer.MAX_VALUE);

    private final TokenBucket throttleBucket;

    public Throttle(long maxRatePerSecond) {
        this.throttleBucket = TokenBuckets.newFixedIntervalRefill(maxRatePerSecond, maxRatePerSecond, 1, TimeUnit.SECONDS);;
    }

    public void consume(long usage){
        throttleBucket.consume(usage);
    }
}
