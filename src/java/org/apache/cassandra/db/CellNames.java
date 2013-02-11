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
package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.cql3.CFDefinition;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.ColumnToCollectionType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.Allocator;
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class CellNames
{
    private CellNames() {}

    public static CellName make(Composite prefix, CFDefinition.Name column)
    {
        switch (column.kind)
        {
            case VALUE_ALIAS:
                return makeDense(prefix);
            case COLUMN_METADATA:
                return new SparseCellName(prefix, column.name.key);
            default:
                throw new IllegalArgumentException();
        }
    }

    public static CellName make(Composite prefix, ByteBuffer column)
    {
        return new SparseCellName(prefix, column);
    }

    public static CellName makeDense(Composite prefix)
    {
        return new DenseCellName(prefix);
    }

    public static CellName makeRowMarker(Composite prefix)
    {
        return new SparseCellName(prefix, ByteBufferUtil.EMPTY_BYTE_BUFFER);
    }

    public static CellName makeCollectionCell(CellName cell, ByteBuffer collectionElement)
    {
        return new SparseCellName.WithCollectionElement(cell.clusteringPrefix(), cell.cql3ColumnName(), collectionElement);
    }


    public static CellNameType fromAbstractType(AbstractType<?> type, boolean isDense)
    {
        if (isDense)
        {
            if (type instanceof CompositeType)
            {
                return new DenseCellNameType(new CompositeCType(((CompositeType)type).types));
            }
            else
            {
                return new DenseCellNameType(new SimpleCType(type));
            }
        }
        else
        {
            if (type instanceof CompositeType)
            {
                List<AbstractType<?>> types = ((CompositeType)type).types;
                if (types.get(types.size() - 1) instanceof ColumnToCollectionType)
                {
                    assert types.get(types.size() - 2) instanceof UTF8Type;
                    return new SparseCellNameType.WithCollection(new CompositeCType(types.subList(0, types.size() - 2)), (ColumnToCollectionType)types.get(types.size() - 1));
                }
                else
                {
                    assert types.get(types.size() - 1) instanceof UTF8Type;
                    return new SparseCellNameType(new CompositeCType(types.subList(0, types.size() - 1)));
                }
            }
            else
            {
                assert type instanceof UTF8Type;
                return new SparseCellNameType(EmptyCType.instance);
            }
        }
    }

    public static CellNameType simpleDenseType(AbstractType<?> type)
    {
        return new DenseCellNameType(new SimpleCType(type));
    }

    public static CellNameType compositeDenseType(AbstractType<?>... types)
    {
        return compositeDenseType(Arrays.asList(types));
    }

    public static CellNameType compositeDenseType(List<AbstractType<?>> types)
    {
        return new DenseCellNameType(new CompositeCType(types));
    }

    public static CellNameType simpleSparseType()
    {
        return new SparseCellNameType(EmptyCType.instance);
    }

    public static CellNameType compositeSparseType(AbstractType<?>... clusteringTypes)
    {
        return compositeSparseType(Arrays.asList(clusteringTypes));
    }

    public static CellNameType compositeSparseType(List<AbstractType<?>> clusteringTypes)
    {
        return new SparseCellNameType(new CompositeCType(clusteringTypes));
    }

    public static CellNameType compositeSparseType(List<AbstractType<?>> clusteringTypes, ColumnToCollectionType collections)
    {
        if (collections == null)
            return compositeSparseType(clusteringTypes);
        else
            return new SparseCellNameType.WithCollection(new CompositeCType(clusteringTypes), collections);
    }

    public static CellName simpleDense(ByteBuffer bb)
    {
        return new DenseCellName(new SimpleComposite(bb));
    }

    public static CellName compositeDense(ByteBuffer... bbs)
    {
        return new DenseCellName(new ListComposite(Arrays.asList(bbs)));
    }

    public static String getColumnsString(CellNameType type, Iterable<Column> columns)
    {
        StringBuilder builder = new StringBuilder();
        for (Column column : columns)
            builder.append(column.getString(type)).append(",");
        return builder.toString();
    }
}
