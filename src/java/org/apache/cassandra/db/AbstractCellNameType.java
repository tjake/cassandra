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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ColumnToCollectionType;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import static org.apache.cassandra.io.sstable.IndexHelper.IndexInfo;

public abstract class AbstractCellNameType extends AbstractCType implements CellNameType
{
    private final Comparator<Column> columnComparator;
    private final Comparator<Column> columnReverseComparator;
    private final Comparator<OnDiskAtom> onDiskAtomComparator;

    private final ISerializer<Composite> compositeSerializer;
    private final ISerializer<CellName> cellSerializer;
    private final ColumnSerializer columnSerializer;
    private final OnDiskAtom.Serializer onDiskAtomSerializer;
    private final IVersionedSerializer<NamesQueryFilter> namesQueryFilterSerializer;
    private final IVersionedSerializer<IDiskAtomFilter> diskAtomFilterSerializer;

    protected AbstractCellNameType()
    {
        columnComparator = new Comparator<Column>()
        {
            public int compare(Column c1, Column c2)
            {
                return AbstractCellNameType.this.compare(c1.name(), c2.name());
            }
        };
        columnReverseComparator = new Comparator<Column>()
        {
            public int compare(Column c1, Column c2)
            {
                return AbstractCellNameType.this.compare(c2.name(), c1.name());
            }
        };
        onDiskAtomComparator = new Comparator<OnDiskAtom>()
        {
            public int compare(OnDiskAtom c1, OnDiskAtom c2)
            {
                int comp = AbstractCellNameType.this.compare(c1.name(), c2.name());
                if (comp != 0)
                    return comp;

                if (c1 instanceof RangeTombstone)
                {
                    if (c2 instanceof RangeTombstone)
                    {
                        RangeTombstone t1 = (RangeTombstone)c1;
                        RangeTombstone t2 = (RangeTombstone)c2;
                        int comp2 = AbstractCellNameType.this.compare(t1.max, t2.max);
                        return comp2 == 0 
                             ? t1.data.compareTo(t2.data)
                             : comp2;
                    }
                    else
                    {
                        return -1;
                    }
                }
                else
                {
                    return c2 instanceof RangeTombstone ? 1 : 0;
                }
            }
        };

        compositeSerializer = new CompositeSerializer(this);
        // A trivial wrapped over compositeSerializer
        cellSerializer = new ISerializer<CellName>()
        {
            public void serialize(CellName c, DataOutput dos) throws IOException
            {
                compositeSerializer.serialize(c, dos);
            }

            public CellName deserialize(DataInput dis) throws IOException
            {
                Composite ct = compositeSerializer.deserialize(dis);
                assert ct instanceof CellName : ct;
                return (CellName)ct;
            }

            public long serializedSize(CellName c, TypeSizes type)
            {
                return compositeSerializer.serializedSize(c, type);
            }
        };
        columnSerializer = new ColumnSerializer(this);
        onDiskAtomSerializer = new OnDiskAtom.Serializer(this);
        namesQueryFilterSerializer = new NamesQueryFilter.Serializer(this);
        diskAtomFilterSerializer = new IDiskAtomFilter.Serializer(this);
    }

    public Comparator<Column> columnComparator()
    {
        return columnComparator;
    }

    public Comparator<Column> columnReverseComparator()
    {
        return columnReverseComparator;
    }

    public Comparator<OnDiskAtom> onDiskAtomComparator()
    {
        return onDiskAtomComparator;
    }

    @Override
    public ISerializer<Composite> serializer()
    {
        return compositeSerializer;
    }

    public ISerializer<CellName> cellSerializer()
    {
        return cellSerializer;
    }

    public ColumnSerializer columnSerializer()
    {
        return columnSerializer;
    }

    public OnDiskAtom.Serializer onDiskAtomSerializer()
    {
        return onDiskAtomSerializer;
    }

    public IVersionedSerializer<NamesQueryFilter> namesQueryFilterSerializer()
    {
        return namesQueryFilterSerializer;
    }

    public IVersionedSerializer<IDiskAtomFilter> diskAtomFilterSerializer()
    {
        return diskAtomFilterSerializer;
    }

    public int clusteringPrefixSize()
    {
        return isDense() ? size() : size() - (hasCollections() ? 2 : 1);
    }

    public boolean hasCollections()
    {
        return false;
    }

    public boolean supportCollections()
    {
        return false;
    }

    public ColumnToCollectionType collectionType()
    {
        throw new UnsupportedOperationException();
    }

    public CellNameType addCollection(ByteBuffer columnName, CollectionType newCollection)
    {
        throw new UnsupportedOperationException();
    }

    public CellName make(Object... components)
    {
        if (components.length != size())
            throw new IllegalArgumentException("Expecting " + size() + " arguments, got " + components.length);

        List<ByteBuffer> rawComponents = new ArrayList<ByteBuffer>(size());
        for (int i = 0; i < size(); i++)
        {
            Object c = components[i];
            rawComponents.add(c instanceof ByteBuffer
                              ? (ByteBuffer)c
                              : ((AbstractType)subtype(i)).decompose(c));
        }
        return makeCellName(rawComponents);
    }

    public CellName cellFromByteBuffer(ByteBuffer bytes)
    {
        return (CellName)fromByteBuffer(bytes);
    }

    protected abstract CellName makeCellName(List<ByteBuffer> components);

    private static class CompositeSerializer implements ISerializer<Composite>
    {
        private final CellNameType type;

        public CompositeSerializer(CellNameType type)
        {
            this.type = type;
        }

        public void serialize(Composite c, DataOutput out) throws IOException
        {
            ByteBufferUtil.writeWithShortLength(c.toByteBuffer(), out);
        }

        public Composite deserialize(DataInput in) throws IOException
        {
            ByteBuffer bb = ByteBufferUtil.readWithShortLength(in);
            if (bb.remaining() == 0)
                throw ColumnSerializer.CorruptColumnException.create(in, bb);
            return type.fromByteBuffer(bb);
        }

        public long serializedSize(Composite c, TypeSizes type)
        {
            ByteBuffer bb = c.toByteBuffer();
            return type.sizeof((short) bb.remaining()) + bb.remaining();
        }
    }
}
