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

package com.jordanwilliams.heftydb.test.integration;

import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.db.HeftyDB;
import com.jordanwilliams.heftydb.db.Record;
import com.jordanwilliams.heftydb.db.Snapshot;
import com.jordanwilliams.heftydb.state.Config;
import com.jordanwilliams.heftydb.test.base.ParameterizedIntegrationTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ConcurrencyTest extends ParameterizedIntegrationTest {

    private static final int THREAD_COUNT = 8;

    public ConcurrencyTest(List<Tuple> tuples, Config config) throws IOException {
        super(tuples, config);
    }

    @Test
    public void dataIntegrityTest() throws Exception {
        final ExecutorService writeExecutor = Executors.newFixedThreadPool(THREAD_COUNT);
        final ExecutorService readExecutor = Executors.newFixedThreadPool(THREAD_COUNT);
        final ExecutorService scanExecutor = Executors.newFixedThreadPool(THREAD_COUNT);

        final ConcurrentLinkedQueue<Tuple> writtenQueue = new ConcurrentLinkedQueue<Tuple>();
        final ConcurrentLinkedQueue<Snapshot> snapshotQueue = new ConcurrentLinkedQueue<Snapshot>();

        for (int i = 0; i < THREAD_COUNT; i++){
            writeExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        for (Tuple tuple : tuples) {
                            db.put(tuple.key().data(), tuple.value().data());
                            writtenQueue.add(tuple);
                        }
                    } catch (IOException e){
                        throw new RuntimeException(e);
                    }
                }
            });

            readExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        Tuple next = writtenQueue.poll();

                        if (next != null){
                            db.get(next.key().data());
                        }
                    } catch (IOException e){
                        throw new RuntimeException(e);
                    }
                }
            });

            scanExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        Snapshot next = snapshotQueue.poll();

                        if (next != null){
                            Iterator<Record> dbIterator = db.ascendingIterator(next);

                            while (dbIterator.hasNext()){
                                dbIterator.next();
                            }
                        }

                        db.compact();
                    } catch (IOException e){
                        throw new RuntimeException(e);
                    }
                }
            });
        }

        writeExecutor.shutdown();
        writeExecutor.awaitTermination(10, TimeUnit.SECONDS);
        readExecutor.shutdownNow();
        scanExecutor.shutdownNow();

        db.close();
        db = HeftyDB.open(config);

        Iterator<Record> dbIterator = db.ascendingIterator(Snapshot.MAX);

        while (dbIterator.hasNext()){
            Assert.assertNotNull("Records can be iterated without error", dbIterator.next());
        }

        db.close();
    }
}
