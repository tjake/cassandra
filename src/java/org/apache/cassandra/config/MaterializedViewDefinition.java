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
package org.apache.cassandra.config;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.cassandra.cql3.ColumnIdentifier;

public class MaterializedViewDefinition
{
    public String baseCfName;
    public String viewName;
    public ColumnIdentifier target;
    public Collection<ColumnIdentifier> included;

    public MaterializedViewDefinition(String baseCfName, String viewName, ColumnIdentifier target, Collection<ColumnIdentifier> included)
    {
        assert target != null;
        this.baseCfName = baseCfName;
        this.viewName = viewName;
        this.target = target;
        this.included = included;
    }

    public boolean selects(ColumnIdentifier def)
    {
        if (target.bytes.compareTo(def.bytes) == 0)
            return true;

        if (included.isEmpty())
            return true;

        for (ColumnIdentifier identifier: included)
        {
            if (identifier.bytes.compareTo(def.bytes) == 0)
                return true;
        }

        return false;
    }

    public void renameColumn(ColumnIdentifier from, ColumnIdentifier to)
    {
        if (target == from)
            target = from;
        else if (!included.isEmpty())
        {
            Collection<ColumnIdentifier> columns = new ArrayList<>();
            for (ColumnIdentifier column: included)
            {
                if (column.bytes.compareTo(to.bytes) == 0)
                {
                    columns.add(to);
                }
                else
                {
                    columns.add(column);
                }
            }
            included = columns;
        }
    }

    public MaterializedViewDefinition copy()
    {
        Collection<ColumnIdentifier> copyIncluded = new ArrayList<>(included.size());
        for (ColumnIdentifier include: included)
        {
            copyIncluded.add(include);
        }
        return new MaterializedViewDefinition(baseCfName, viewName, target, copyIncluded);
    }
}
