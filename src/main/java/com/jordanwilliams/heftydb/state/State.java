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

import com.jordanwilliams.heftydb.table.Table;

import java.util.Collection;

public class State {

    private final Tables tables;
    private final Snapshots snapshots;
    private final Config config;
    private final Files files;

    public State(Collection<Table> tables, Config config, Files files, long currentSnapshotId) {
        this.snapshots = new Snapshots(currentSnapshotId);
        this.tables = new Tables(tables);
        this.config = config;
        this.files = files;
    }

    public Files files(){
        return files;
    }

    public Tables tables() {
        return tables;
    }

    public Snapshots snapshots() {
        return snapshots;
    }

    public Config config() {
        return config;
    }
}
