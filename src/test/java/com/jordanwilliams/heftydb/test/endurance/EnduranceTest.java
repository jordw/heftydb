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

package com.jordanwilliams.heftydb.test.endurance;

import com.jordanwilliams.heftydb.db.DB;
import com.jordanwilliams.heftydb.db.HeftyDB;
import com.jordanwilliams.heftydb.db.Record;
import com.jordanwilliams.heftydb.db.Snapshot;
import com.jordanwilliams.heftydb.test.generator.ConfigGenerator;
import com.jordanwilliams.heftydb.test.generator.KeyValueGenerator;
import com.jordanwilliams.heftydb.test.helper.StopWatch;
import com.jordanwilliams.heftydb.test.helper.TestFileHelper;
import com.jordanwilliams.heftydb.util.ByteBuffers;

import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class EnduranceTest {

    private static final int THREAD_COUNT = 16;
    private static final int RUNTIME_MINUTES = 60;
    private static final int LOAD_LEVEL = 10;
    private static final int VALUE_SIZE = 100;
    private static final int THREAD_SLEEP_TIME = 10;

    public static void main(String[] args) throws Exception {
        TestFileHelper.createTestDirectory();
        TestFileHelper.cleanUpTestFiles();

        final AtomicLong maxSnapshotId = new AtomicLong();
        final AtomicLong maxKey = new AtomicLong();
        final AtomicLong maxVisibleKey = new AtomicLong();
        final AtomicBoolean finished = new AtomicBoolean();

        final KeyValueGenerator keyValueGenerator = new KeyValueGenerator();
        final Random random = new Random(System.nanoTime());

        final ExecutorService writeExecutor = Executors.newFixedThreadPool(THREAD_COUNT);
        final ExecutorService readExecutor = Executors.newFixedThreadPool(THREAD_COUNT);
        final ExecutorService scanExecutor = Executors.newFixedThreadPool(THREAD_COUNT);

        final DB db = HeftyDB.open(ConfigGenerator.enduranceConfig());

        for (int i = 0; i < THREAD_COUNT; i++) {
            writeExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        while (true) {
                            if (finished.get()) {
                                return;
                            }

                            for (int i = 0; i < LOAD_LEVEL; i++) {
                                String nextKey = Long.toString(maxKey.incrementAndGet());
                                Snapshot maxSnapshot = db.put(ByteBuffers.fromString(nextKey),
                                        keyValueGenerator.testValue(VALUE_SIZE));

                                maxVisibleKey.incrementAndGet();

                                long currentMaxSnapshotId = maxSnapshotId.get();

                                if (maxSnapshot.id() > currentMaxSnapshotId) {
                                    maxSnapshotId.compareAndSet(currentMaxSnapshotId, maxSnapshot.id());
                                }
                            }

                            Thread.sleep(THREAD_SLEEP_TIME);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });

            readExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        while (true) {
                            if (finished.get()) {
                                return;
                            }

                            for (int i = 0; i < LOAD_LEVEL; i++) {
                                if (maxVisibleKey.get() > 0){
                                    long randomKey = (long) (random.nextDouble()*(maxVisibleKey.get()));
                                    String nextKey = Long.toString(randomKey);
                                    db.get(ByteBuffers.fromString(nextKey));
                                }
                            }

                            Thread.sleep(THREAD_SLEEP_TIME);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });

            scanExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        Iterator<Record> scanIterator = db.ascendingIterator(new Snapshot(maxSnapshotId.get()));
                        long maxScanSnapshotId = 0;

                        while (true) {
                            if (finished.get()) {
                                return;
                            }

                            for (int i = 0; i < LOAD_LEVEL; i++) {
                                if (scanIterator.hasNext()) {
                                    Record record = scanIterator.next();
                                    maxScanSnapshotId = Math.max(maxScanSnapshotId, record.snapshot().id());
                                } else {
                                    scanIterator = db.ascendingIterator(new Snapshot(maxSnapshotId.get()));
                                }
                            }

                            Thread.sleep(THREAD_SLEEP_TIME);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }

        StopWatch watch = StopWatch.start();

        while (watch.elapsedMinutes() < RUNTIME_MINUTES) {
            Thread.sleep(1000);
        }

        finished.set(true);

        writeExecutor.shutdown();
        readExecutor.shutdown();
        scanExecutor.shutdown();

        writeExecutor.awaitTermination(30, TimeUnit.SECONDS);
        readExecutor.awaitTermination(30, TimeUnit.SECONDS);
        scanExecutor.awaitTermination(30, TimeUnit.SECONDS);

        db.close();

        TestFileHelper.cleanUpTestFiles();
    }
}
