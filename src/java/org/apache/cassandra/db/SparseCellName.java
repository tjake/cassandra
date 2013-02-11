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

import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.utils.Allocator;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SparseCellName extends AbstractCellName
{
    protected final ByteBuffer columnName;

    SparseCellName(Composite prefix, ByteBuffer columnName)
    {
        super(prefix);
        assert prefix != null && columnName != null;
        assert prefix.eoc() == Composite.EOC.NONE;
        this.columnName = columnName;
    }

    @Override
    public int size()
    {
        return prefix.size() + 1;
    }

    @Override
    public ByteBuffer get(int i)
    {
        return i == prefix.size() ? columnName : prefix.get(i);
    }

    @Override
    public ByteBuffer cql3ColumnName()
    {
        return columnName;
    }

    public boolean isSameCQL3RowThan(CellName other)
    {
        if (!(other instanceof SparseCellName))
            return false;

        SparseCellName o = (SparseCellName)other;
        return prefix.equals(o.prefix);
    }

    // intern or copy
    public CellName copy(Allocator allocator)
    {
        return new SparseCellName(prefix.copy(allocator), allocator.clone(columnName));
    }

    public static class WithCollectionElement extends SparseCellName
    {
        private final ByteBuffer collectionElement;

        WithCollectionElement(Composite prefix, ByteBuffer columnName, ByteBuffer collectionElement)
        {
            super(prefix, columnName);
            assert collectionElement != null;
            this.collectionElement = collectionElement;
        }

        @Override
        public int size()
        {
            return super.size() + 1;
        }

        @Override
        public ByteBuffer get(int i)
        {
            return i == super.size() ? collectionElement : super.get(i);
        }

        @Override
        public ByteBuffer collectionElement()
        {
            return collectionElement;
        }

        @Override
        public boolean isCollectionCell()
        {
            return true;
        }

        @Override
        public CellName copy(Allocator allocator)
        {
            return new WithCollectionElement(prefix.copy(allocator), allocator.clone(columnName), allocator.clone(collectionElement));
        }
    }
}
