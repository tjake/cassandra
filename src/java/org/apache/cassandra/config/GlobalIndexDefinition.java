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
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.index.GlobalIndex;

public class GlobalIndexDefinition
{
    public String indexName;
    public ColumnIdentifier target;
    public Collection<ColumnIdentifier> denormalized;

    public GlobalIndexDefinition(String indexName, ColumnIdentifier target, Collection<ColumnIdentifier> denormalized)
    {
        assert target != null;
        this.indexName = indexName;
        this.target = target;
        this.denormalized = denormalized;
    }

    public GlobalIndex resolve(CFMetaData cfm)
    {
        ColumnDefinition targetCd = cfm.getColumnDefinition(target);
        assert targetCd != null;

        Collection<ColumnDefinition> denormalizedCds = new ArrayList<>();
        for (ColumnIdentifier identifier: denormalized)
        {
            ColumnDefinition cfDef = cfm.getColumnDefinition(identifier);
            assert cfDef != null;
            denormalizedCds.add(cfDef);
        }

        return new GlobalIndex(targetCd, denormalizedCds, Keyspace.open(cfm.ksName).getColumnFamilyStore(cfm.cfName));
    }
}
