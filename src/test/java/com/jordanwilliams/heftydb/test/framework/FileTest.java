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

package com.jordanwilliams.heftydb.test.framework;

import com.jordanwilliams.heftydb.test.util.TestFileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;

public class FileTest {

    @BeforeClass
    public static void beforeClass() throws IOException {
        TestFileUtils.createTestDirectory();
        TestFileUtils.cleanUpTestFiles();
    }

    @AfterClass
    public static void afterClass() throws IOException {
        TestFileUtils.cleanUpTestFiles();
    }

    @Before
    public void beforeTest() throws IOException {
        TestFileUtils.createTestDirectory();
    }

    @After
    public void afterTest() throws IOException {
        TestFileUtils.cleanUpTestFiles();
    }
}
