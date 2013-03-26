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
package org.apache.cassandra.db.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.ISSTableColumnIterator;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;

/**
 * Given an implementation-specific description of what columns to look for, provides methods
 * to extract the desired columns from a Memtable, SSTable, or SuperColumn.  Either the get*ColumnIterator
 * methods will be called, or filterSuperColumn, but not both on the same object.  QueryFilter
 * takes care of putting the two together if subcolumn filtering needs to be done, based on the
 * querypath that it knows (but that IFilter implementations are oblivious to).
 */
public interface IDiskAtomFilter
{
    /**
     * returns an iterator that returns columns from the given memtable
     * matching the Filter criteria in sorted order.
     */
    public OnDiskAtomIterator getMemtableColumnIterator(ColumnFamily cf, DecoratedKey key);

    /**
     * Get an iterator that returns columns from the given SSTable using the opened file
     * matching the Filter criteria in sorted order.
     * @param sstable
     * @param file Already opened file data input, saves us opening another one
     * @param key The key of the row we are about to iterate over
     */
    public ISSTableColumnIterator getSSTableColumnIterator(SSTableReader sstable, FileDataInput file, DecoratedKey key, RowIndexEntry indexEntry);

    /**
     * returns an iterator that returns columns from the given SSTable
     * matching the Filter criteria in sorted order.
     */
    public ISSTableColumnIterator getSSTableColumnIterator(SSTableReader sstable, DecoratedKey key);

    /**
     * collects columns from reducedColumns into returnCF.  Termination is determined
     * by the filter code, which should have some limit on the number of columns
     * to avoid running out of memory on large rows.
     */
    public void collectReducedColumns(ColumnFamily container, Iterator<Column> reducedColumns, int gcBefore);

    public Comparator<Column> getColumnComparator(CellNameType comparator);

    public boolean isReversed();
    public void updateColumnsLimit(int newLimit);

    public int getLiveCount(ColumnFamily cf);

    public IDiskAtomFilter cloneShallow();
    public boolean maySelectPrefix(Comparator<Composite> cmp, Composite prefix);

    public static class Serializer implements IVersionedSerializer<IDiskAtomFilter>
    {
        private final CellNameType type;

        public Serializer(CellNameType type)
        {
            this.type = type;
        }

        public void serialize(IDiskAtomFilter filter, DataOutput out, int version) throws IOException
        {
            if (filter instanceof SliceQueryFilter)
            {
                out.writeByte(0);
                type.sliceQueryFilterSerializer().serialize((SliceQueryFilter)filter, out, version);
            }
            else
            {
                out.writeByte(1);
                type.namesQueryFilterSerializer().serialize((NamesQueryFilter)filter, out, version);
            }
        }

        public IDiskAtomFilter deserialize(DataInput in, int version) throws IOException
        {
            int b = in.readByte();
            if (b == 0)
            {
                return type.sliceQueryFilterSerializer().deserialize(in, version);
            }
            else
            {
                assert b == 1;
                return type.namesQueryFilterSerializer().deserialize(in, version);
            }
        }

        public long serializedSize(IDiskAtomFilter filter, TypeSizes typeSizes, int version)
        {
            int size = 1;
            if (filter instanceof SliceQueryFilter)
                size += type.sliceQueryFilterSerializer().serializedSize((SliceQueryFilter)filter, typeSizes, version);
            else
                size += type.namesQueryFilterSerializer().serializedSize((NamesQueryFilter)filter, typeSizes, version);
            return size;
        }
    }
}
