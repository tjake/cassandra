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
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;

public abstract class MaterializedViewSelectorOnCollection extends MaterializedViewSelector
{
    protected MaterializedViewSelectorOnCollection(ColumnFamilyStore baseCfs, ColumnDefinition columnDefinition)
    {
        super(baseCfs, columnDefinition);
    }

    public boolean isBasePrimaryKey()
    {
        return false;
    }

    public boolean selects(CellName name)
    {
        AbstractType<?> comp = baseCfs.metadata.getColumnDefinitionComparator(columnDefinition);
        return name.size() > columnDefinition.position()
               && comp.compare(name.get(columnDefinition.position()), columnDefinition.name.bytes) == 0;
    }

    public ByteBuffer getSingle(ByteBuffer partitionKey, Cell cell)
    {
        throw new InvalidRequestException("Cannot get single value from Collection MVS");
    }
}
