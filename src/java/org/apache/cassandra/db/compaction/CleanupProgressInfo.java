/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.compaction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.schema.TableId;

public class CleanupProgressInfo
{
    private final Set<TableId> tablesToCleanUp;
    private final Set<TableId> tablesCompleted = new LinkedHashSet<>();
    private int sstablesToCleanUp = 0;
    private int sstablesCompleted = 0;
    private TableId currentTableId;

    private static final Logger logger = LoggerFactory.getLogger(CleanupProgressInfo.class);

    public CleanupProgressInfo(Set<TableId> tablesToCleanUp)
    {
        this.tablesToCleanUp = tablesToCleanUp;
    }

    public void onCleanupNewTable(TableId currTable, int sstablesToCleanUp)
    {
        assert this.sstablesToCleanUp == 0;
        this.currentTableId = currTable;
        this.sstablesToCleanUp = sstablesToCleanUp;
    }

    public void onCleanupNewSSTable()
    {
        sstablesCompleted++;
        if (sstablesToCleanUp == sstablesCompleted)
        {
            tablesCompleted.add(currentTableId);
            currentTableId = null;
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CleanupProgressInfo that = (CleanupProgressInfo) o;
        return sstablesToCleanUp == that.sstablesToCleanUp && sstablesCompleted == that.sstablesCompleted && Objects.equals(tablesToCleanUp, that.tablesToCleanUp) && Objects.equals(tablesCompleted, that.tablesCompleted) && Objects.equals(currentTableId, that.currentTableId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tablesToCleanUp, tablesCompleted, sstablesToCleanUp, sstablesCompleted, currentTableId);
    }

    public Map<String, String> getMapRepresentation(){
        Map<String, String> varMap = new HashMap<>();

        Set<String> tablesToCleanUpStrings = new HashSet<>();
        tablesToCleanUp.forEach(tableId -> {
            String tableIdString = tableId.toString();
            tablesToCleanUpStrings.add(tableIdString);
        });
        varMap.put("tablesToCleanUp", String.join(",", tablesToCleanUpStrings));

        Set<String> tablesCompletedStrings = new HashSet<>();
        tablesCompleted.forEach(tableId -> {
            String tableIdString = tableId.toString();
            tablesCompletedStrings.add(tableIdString);
        });
        varMap.put("tablesCompleted", String.join(",", tablesCompletedStrings));

        varMap.put("sstablesToCleanUp", String.valueOf(sstablesToCleanUp));
        varMap.put("sstablesCompleted", String.valueOf(sstablesCompleted));
        varMap.put("currentTableId", currentTableId.toString());
        return varMap;
    }
    @Override
    public String toString()
    {
        return "CleanupProgressInfo{" +
               "tablesToCleanUp=" + tablesToCleanUp +
               ", tablesCompleted=" + tablesCompleted +
               ", sstablesToCleanUp=" + sstablesToCleanUp +
               ", sstablesCompleted=" + sstablesCompleted +
               ", currentTableId=" + currentTableId +
               '}';
    }
}
