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

package com.jordanwilliams.heftydb.test.helper;

public final class StopWatch {

    private final long startTime;

    public StopWatch() {
        startTime = System.nanoTime();
    }

    public long elapsed() {
        return System.nanoTime() - startTime;
    }

    public double elapsedMicros() {
        return (double) elapsed() / 1000;
    }

    public double elapsedMillis() {
        return (double) elapsed() / 1000000;
    }

    public double elapsedSeconds() {
        return (double) elapsed() / 1000000000;
    }

    public double elapsedMinutes() {
        return elapsedSeconds() / 60;
    }

    public static StopWatch start() {
        return new StopWatch();
    }
}