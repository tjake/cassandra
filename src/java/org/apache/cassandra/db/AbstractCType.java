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
import java.util.List;

import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AbstractCompositeType;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import static org.apache.cassandra.io.sstable.IndexHelper.IndexInfo;

public abstract class AbstractCType implements CType
{
    private final Comparator<Composite> reverseComparator;
    private final Comparator<IndexInfo> indexComparator;
    private final Comparator<IndexInfo> indexReverseComparator;

    private final ISerializer<Composite> serializer;

    private final ISerializer<IndexInfo> indexSerializer;
    private final IVersionedSerializer<ColumnSlice> sliceSerializer;
    private final IVersionedSerializer<SliceQueryFilter> sliceQueryFilterSerializer;
    private final DeletionInfo.Serializer deletionInfoSerializer;
    private final RangeTombstone.Serializer rangeTombstoneSerializer;
    private final RowIndexEntry.Serializer rowIndexEntrySerializer;

    protected AbstractCType()
    {
        reverseComparator = new Comparator<Composite>()
        {
            public int compare(Composite c1, Composite c2)
            {
                return AbstractCType.this.compare(c2, c1);
            }
        };
        indexComparator = new Comparator<IndexInfo>()
        {
            public int compare(IndexInfo o1, IndexInfo o2)
            {
                return AbstractCType.this.compare(o1.lastName, o2.lastName);
            }
        };
        indexReverseComparator = new Comparator<IndexInfo>()
        {
            public int compare(IndexInfo o1, IndexInfo o2)
            {
                return AbstractCType.this.compare(o1.firstName, o2.firstName);
            }
        };

        serializer = new Serializer(this);

        indexSerializer = new IndexInfo.Serializer(this);
        sliceSerializer = new ColumnSlice.Serializer(this);
        sliceQueryFilterSerializer = new SliceQueryFilter.Serializer(this);
        deletionInfoSerializer = new DeletionInfo.Serializer(this);
        rangeTombstoneSerializer = new RangeTombstone.Serializer(this);
        rowIndexEntrySerializer = new RowIndexEntry.Serializer(this);
    }

    public int compare(Composite c1, Composite c2)
    {
        if (c1 == null)
            return c2 == null ? 0 : -1;

        ByteBuffer previous = null;
        int i;
        for (i = 0; i < Math.min(c1.size(), c2.size()); i++)
        {
            AbstractType<?> comparator = subtype(i);
            ByteBuffer value1 = c1.get(i);
            ByteBuffer value2 = c2.get(i);

            int cmp = comparator.compareCollectionMembers(value1, value2, previous);
            if (cmp != 0)
                return cmp;

            previous = value1;
        }

        if (c1.size() == c2.size())
        {
            if (c1.eoc() != c2.eoc())
            {
                switch (c1.eoc())
                {
                    case START: return -1;
                    case END:   return 1;
                    case NONE:  return c2.eoc() == Composite.EOC.START ? 1 : -1;
                }
            }
            return 0;
        }

        if (i == c1.size())
        {
            return c1.eoc() == Composite.EOC.END ? 1 : -1;
        }
        else
        {
            assert i == c2.size();
            return c2.eoc() == Composite.EOC.END ? -1 : 1;
        }
    }

    /**
     * @throws IllegalArgumentException if the bytes are not a valid composite
     */
    static Composite.EOC readCompositeElements(ByteBuffer bytes, List<ByteBuffer> elements)
    {
        ByteBuffer bb = bytes.duplicate();
        byte eoc = 0;
        while (bb.remaining() > 0)
        {
            if (bb.remaining() < 2)
                throw new IllegalArgumentException("Not enough bytes to read value size of component " + elements.size());
            int length = AbstractCompositeType.getShortLength(bb);

            if (bb.remaining() < length)
                throw new IllegalArgumentException("Not enough bytes to read value of component " + elements.size());
            elements.add(AbstractCompositeType.getBytes(bb, length));

            if (bb.remaining() == 0)
                throw new IllegalArgumentException("Not enough bytes to read the end-of-component byte of component" + elements.size());
            eoc = bb.get();
            if (eoc != 0 && bb.remaining() > 0)
                throw new IllegalArgumentException("Invalid bytes remaining after an end-of-component at component" + elements.size());
        }
        return Composite.EOC.from(eoc);
    }

    public void validate(Composite name)
    {
        ByteBuffer previous = null;
        for (int i = 0; i < name.size(); i++)
        {
            AbstractType<?> comparator = subtype(i);
            ByteBuffer value = name.get(i);
            comparator.validateCollectionMember(value, previous);
            previous = null;
        }
    }

    public boolean isCompatibleWith(CType previous)
    {
        if (this == previous)
            return true;

        // Extending with new components is fine, shrinking is not
        if (size() < previous.size())
            return false;

        for (int i = 0; i < previous.size(); i++)
        {
            AbstractType<?> tprev = previous.subtype(i);
            AbstractType<?> tnew = subtype(i);
            if (!tnew.isCompatibleWith(tprev))
                return false;
        }
        return true;
    }

    public String getString(Composite c)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < c.size(); i++)
        {
            if (i > 0)
                sb.append(":");
            sb.append(AbstractCompositeType.escape(subtype(i).getString(c.get(i))));
        }
        switch (c.eoc())
        {
            case START:
                sb.append(":_");
                break;
            case END:
                sb.append(":!");
                break;
        }
        return sb.toString();
    }

    public ISerializer<Composite> serializer()
    {
        return serializer;
    }

    // For now, just serialize old-style. We can get fancy later.
    private static class Serializer implements ISerializer<Composite>
    {
        private final CType type;

        public Serializer(CType type)
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

    public Comparator<Composite> reverseComparator()
    {
        return reverseComparator;
    }

    public Comparator<IndexInfo> indexComparator()
    {
        return indexComparator;
    }

    public Comparator<IndexInfo> indexReverseComparator()
    {
        return indexReverseComparator;
    }

    public ISerializer<IndexInfo> indexSerializer()
    {
        return indexSerializer;
    }

    public IVersionedSerializer<ColumnSlice> sliceSerializer()
    {
        return sliceSerializer;
    }

    public IVersionedSerializer<SliceQueryFilter> sliceQueryFilterSerializer()
    {
        return sliceQueryFilterSerializer;
    }

    public DeletionInfo.Serializer deletionInfoSerializer()
    {
        return deletionInfoSerializer;
    }

    public RangeTombstone.Serializer rangeTombstoneSerializer()
    {
        return rangeTombstoneSerializer;
    }

    public RowIndexEntry.Serializer rowIndexEntrySerializer()
    {
        return rowIndexEntrySerializer;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (o == null)
            return false;

        if (!getClass().equals(o.getClass()))
            return false;

        CType c = (CType)o;
        if (size() != c.size())
            return false;

        for (int i = 0; i < size(); i++)
        {
            if (!subtype(i).equals(c.subtype(i)))
                return false;
        }
        return true;
    }

    @Override
    public int hashCode()
    {
        int h = 31;
        for (int i = 0; i < size(); i++)
            h += subtype(i).hashCode();
        return h + getClass().hashCode();
    }

    @Override
    public String toString()
    {
        return asAbstractType().toString();
    }
}
