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

package com.jordanwilliams.heftydb.compact;

import com.jordanwilliams.heftydb.compact.planner.CompactionPlanner;
import com.jordanwilliams.heftydb.compact.planner.FullCompactionPlanner;
import com.jordanwilliams.heftydb.compact.planner.SizeTieredCompactionPlanner;
import com.jordanwilliams.heftydb.state.Tables;

import java.util.Collections;

public enum CompactionStrategies implements CompactionStrategy {

    SIZE_TIERED_COMPACTION_STRATEGY {
        @Override
        public CompactionPlanner initialize(Tables tables) {
            return new SizeTieredCompactionPlanner(tables);
        }
    },

    NULL_COMPACTION_STRATEGY {
        @Override
        public CompactionPlanner initialize(Tables tables) {
            return new CompactionPlanner() {
                @Override
                public CompactionPlan planCompaction() {
                    return new CompactionPlan(Collections.<CompactionTask>emptyList());
                }

                @Override
                public boolean needsCompaction() {
                    return false;
                }
            };
        }
    },

    FULL_COMPACTION_STRATEGY {
        @Override
        public CompactionPlanner initialize(Tables tables) {
            return new FullCompactionPlanner(tables);
        }
    };

    @Override
    public abstract CompactionPlanner initialize(Tables tables);
}
