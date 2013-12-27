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

package com.jordanwilliams.heftydb.state;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.SortedSet;
import java.util.TreeSet;

public class DataFiles {

    private static final String TABLE_EXT = ".table";
    private static final String LOG_EXT = ".log";
    private static final String INDEX_EXT = ".index";
    private static final String FILTER_EXT = ".filter";
    private static final String TEMP_FILE_EXT = ".temp";

    private final Path logDirectory;
    private final Path tableDirectory;

    public DataFiles(Path tableDirectory, Path logDirectory) {
        this.tableDirectory = tableDirectory;
        this.logDirectory = logDirectory;
    }

    public Path tempTablePath(long tableId) {
        return tableDirectory.resolve(tableId + TABLE_EXT + TEMP_FILE_EXT);
    }

    public Path tempIndexPath(long tableId) {
        return tableDirectory.resolve(tableId + INDEX_EXT + TEMP_FILE_EXT);
    }

    public Path tempFilterPath(long tableId) {
        return tableDirectory.resolve(tableId + FILTER_EXT + TEMP_FILE_EXT);
    }

    public Path tablePath(long tableId) {
        return tableDirectory.resolve(tableId + TABLE_EXT);
    }

    public Path indexPath(long tableId) {
        return tableDirectory.resolve(tableId + INDEX_EXT);
    }

    public Path filterPath(long tableId) {
        return tableDirectory.resolve(tableId + FILTER_EXT);
    }

    public Path logPath(long tableId) {
        return tableDirectory.resolve(tableId + LOG_EXT);
    }

    public SortedSet<Path> tableFilePaths() throws IOException {
        return filePathsForExtension(tableDirectory, TABLE_EXT);
    }

    public SortedSet<Path> logFilePaths() throws IOException {
        return filePathsForExtension(tableDirectory, LOG_EXT);
    }

    public SortedSet<Path> tempFilePaths() throws IOException {
        return filePathsForExtension(tableDirectory, TEMP_FILE_EXT);
    }

    private SortedSet<Path> filePathsForExtension(Path directory, String extension) throws IOException {
        SortedSet<Path> sortedFilePaths = new TreeSet<Path>();
        DirectoryStream<Path> filePaths = java.nio.file.Files.newDirectoryStream(directory, extension);

        for (Path filePath : filePaths) {
            sortedFilePaths.add(filePath);
        }

        return sortedFilePaths;
    }
}
