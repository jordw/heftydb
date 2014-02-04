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

package com.jordanwilliams.heftydb.test.unit.table.file;

import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.state.Paths;
import com.jordanwilliams.heftydb.table.file.TableBloomFilter;
import com.jordanwilliams.heftydb.test.base.ParameterizedRecordTest;
import com.jordanwilliams.heftydb.test.generator.ConfigGenerator;
import com.jordanwilliams.heftydb.table.file.TableBloomFilterWriter;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TableBloomFilterTest extends ParameterizedRecordTest {

    private final TableBloomFilter bloomFilter;

    public TableBloomFilterTest(List<Tuple> testTuples) throws Exception {
        super(testTuples);

        Paths paths = ConfigGenerator.testPaths();
        TableBloomFilterWriter filterWriter = TableBloomFilterWriter.open(1, paths, tuples.size());

        for (Tuple tuple : tuples){
            filterWriter.write(tuple.key());
        }

        filterWriter.finish();

        this.bloomFilter = TableBloomFilter.read(1, paths);
    }

    @Test
    public void mightContainTest(){
        for (Tuple tuple : tuples){
            Assert.assertTrue("Filter contains the key", bloomFilter.mightContain(tuple.key()));
        }
    }
}
