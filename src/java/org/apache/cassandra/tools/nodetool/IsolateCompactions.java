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

import io.airlift.command.Command;
import io.airlift.command.Option;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(name = "isolatecompactions", description = "Put compaction threads on their own set of cores")
public class IsolateCompactions extends NodeTool.NodeToolCmd
{
    @Option(title = "ratio",
    name = {"-r", "--ratio"},
    description = "Specify the ratio of cores to use only for compactions (must be < 1.0), 0 disables",
    required = true)
    private Double ratio;

    protected void execute(NodeProbe probe)
    {
        if (ratio >= 1)
            throw new IllegalArgumentException("Ratio must be < 1.0");

        if (ratio < 0)
            throw new IllegalArgumentException("Ratio must be > 0");

        probe.setCompactionIsolationRatio(ratio);
    }
}
