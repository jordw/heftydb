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

import com.google.common.util.concurrent.RateLimiter;

/**
 * An I/O rate limiter that uses the Token Bucket algorithm to limit throughput.
 */
public class Throttle {

    public static Throttle MAX = new Throttle(Integer.MAX_VALUE);

    private final RateLimiter rateLimiter;

    public Throttle(long maxRatePerSecond) {
        this.rateLimiter = RateLimiter.create(maxRatePerSecond);
    }

    public void consume(int usage) {
        rateLimiter.acquire(usage);
    }
}
