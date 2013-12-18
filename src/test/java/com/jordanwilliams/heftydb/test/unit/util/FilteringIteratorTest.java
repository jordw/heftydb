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

package com.jordanwilliams.heftydb.test.unit.util;

import com.jordanwilliams.heftydb.util.FilteringIterator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;

public class FilteringIteratorTest {

    private static final String[] SOURCE_ARRAY = {"a", "b", "c", "d"};
    private static final String[] FILTERED_ARRAY = {"a", "b", "d"};

    @Test
    public void simpleFilterTest(){
        Iterator<String> sourceIter = Arrays.asList(SOURCE_ARRAY).iterator();
        Iterator<String> filteredIter = Arrays.asList(FILTERED_ARRAY).iterator();

        FilteringIterator<String> filteringIterator = new FilteringIterator<String>(new FilteringIterator.Filter<String>() {
            @Override
            public boolean accept(String data) {
                if (data.equals("c")){
                    return false;
                }

                return true;
            }
        }, sourceIter);

        while(filteringIterator.hasNext()){
            Assert.assertEquals("Iterators match", filteredIter.next(), filteringIterator.next());
        }
    }
}
