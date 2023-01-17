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
package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.airlift.airline.Option;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "cleanup", description = "Triggers the immediate cleanup of keys no longer belonging to a node. By default, clean all keyspaces")
public class Cleanup extends NodeToolCmd
{
    @Arguments(usage = "[<keyspace> <tables>...]", description = "The keyspace followed by one or many tables")
    private List<String> args = new ArrayList<>();

    @Option(title = "jobs",
            name = {"-j", "--jobs"},
            description = "Number of sstables to cleanup simultanously, set to 0 to use all available compaction threads")
    private int jobs = 2;

    @Option(title = "status",
            name = {"-s", "--status"},
           description = "If a cleanup process is taking place, it will show the current amount of SSTables cleaned up")
    private boolean status = false;

    @Override
    public void execute(NodeProbe probe)
    {
        List<String> keyspaces = parseOptionalKeyspace(args, probe, KeyspaceSet.NON_LOCAL_STRATEGY);
        String[] tableNames = parseOptionalTables(args);

        for (String keyspace : keyspaces)
        {
            if (SchemaConstants.isLocalSystemKeyspace(keyspace))
                continue;

            try
            {
                if (status)
                {
                    // Status flag is active, show status of current cleanup or error if no cleanup is happening
                    try{
                        Map<String, String> cleanupProgress = probe.getCleanupProgress();
                        probe.output().err.println("Cleanup Process Running with status flag, fetching progess...");
                        probe.output().out.println(cleanupProgress.toString());
                    } catch (NullPointerException e){
                        probe.output().err.println("Cleanup Process not Running, must be running to fetch status!");
                        break;
                    }
                }
                else
                {
                    probe.output().err.println("Cleanup Process Running without status flag, starting keyspace cleanup...");
                    probe.forceKeyspaceCleanup(probe.output().out, jobs, keyspace, tableNames);
                }
            }
            catch (Exception e)
            {
                throw new RuntimeException("Error occurred during cleanup", e);
            }
        }
    }
}
