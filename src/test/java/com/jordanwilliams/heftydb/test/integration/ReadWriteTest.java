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

import com.jordanwilliams.heftydb.db.HeftyDB;
import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.test.base.ParameterizedIntegrationTest;
import org.junit.Test;

import java.util.List;

public class ReadWriteTest extends ParameterizedIntegrationTest {

    public ReadWriteTest(HeftyDB db, List<Record> records) {
        super(db, records);
    }

    @Test
    public void readWriteTest() throws Exception {

    }
}
