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
package org.apache.cassandra.db.view;

import java.nio.ByteBuffer;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.marshal.CollectionType;

public abstract class MaterializedViewSelector
{
    public final ColumnDefinition columnDefinition;
    protected MaterializedViewSelector(ColumnDefinition columnDefinition)
    {
        this.columnDefinition = columnDefinition;
    }

    public static MaterializedViewSelector create(ColumnFamilyStore baseCfs, ColumnIdentifier identifier)
    {
        ColumnDefinition definition = baseCfs.metadata.getColumnDefinition(identifier);

        if (definition.type.isCollection() && definition.type.isMultiCell())
        {
            switch (((CollectionType)definition.type).kind)
            {
                case LIST:
                    return new MaterializedViewSelectorOnList(definition);
                case SET:
                    return new MaterializedViewSelectorOnSet(definition);
                case MAP:
                    return new MaterializedViewSelectorOnMap(definition);
            }
        }

        switch (definition.kind)
        {
            case CLUSTERING_COLUMN:
                return new MaterializedViewSelectorOnClusteringColumn(definition);
            case REGULAR:
                return new MaterializedViewSelectorOnRegularColumn(baseCfs, definition);
            case PARTITION_KEY:
                return new MaterializedViewSelectorOnPartitionKey(baseCfs, definition);
        }
        throw new AssertionError();
    }

    /**
     * @return True if this is part of the base's primary key, false otherwise.
     */
    public abstract boolean isBasePrimaryKey();

    public abstract boolean selects(CellName cellName);
}
