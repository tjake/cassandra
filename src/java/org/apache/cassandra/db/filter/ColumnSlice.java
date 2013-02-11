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

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.utils.Allocator;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ColumnSlice
{
    public static final ColumnSlice ALL_COLUMNS = new ColumnSlice(Composites.EMPTY, Composites.EMPTY);
    public static final ColumnSlice[] ALL_COLUMNS_ARRAY = new ColumnSlice[]{ ALL_COLUMNS };

    public final Composite start;
    public final Composite finish;

    public ColumnSlice(Composite start, Composite finish)
    {
        assert start != null && finish != null;
        this.start = start;
        this.finish = finish;
    }

    /**
     * Validate an array of column slices.
     * To be valid, the slices must be sorted and non-overlapping and each slice must be valid.
     *
     * @throws IllegalArgumentException if the input slices are not valid.
     */
    public static void validate(ColumnSlice[] slices, CType comparator, boolean reversed)
    {
        for (int i = 0; i < slices.length; i++)
        {
            ColumnSlice slice = slices[i];
            validate(slice, comparator, reversed);
            if (i > 0)
            {
                if (slices[i - 1].finish.isEmpty() || slice.start.isEmpty())
                    throw new IllegalArgumentException("Invalid column slices: slices must be sorted and non-overlapping");

                int cmp = comparator.compare(slices[i -1].finish, slice.start);
                if (reversed ? cmp <= 0 : cmp >= 0)
                    throw new IllegalArgumentException("Invalid column slices: slices must be sorted and non-overlapping");
            }
        }
    }

    /**
     * Validate a column slices.
     * To be valid, the slice start must sort before the slice end.
     *
     * @throws IllegalArgumentException if the slice is not valid.
     */
    public static void validate(ColumnSlice slice, CType comparator, boolean reversed)
    {
        Comparator<Composite> orderedComparator = reversed ? comparator.reverseComparator() : comparator;
        if (!slice.start.isEmpty() && !slice.finish.isEmpty() && orderedComparator.compare(slice.start, slice.finish) > 0)
            throw new IllegalArgumentException("Slice finish must come after start in traversal order");
    }

    public boolean includes(Comparator<Composite> cmp, Composite name)
    {
        return cmp.compare(start, name) <= 0 && (finish.isEmpty() || cmp.compare(finish, name) >= 0);
    }

    @Override
    public final int hashCode()
    {
        int hashCode = 31 + start.hashCode();
        return 31*hashCode + finish.hashCode();
    }

    @Override
    public final boolean equals(Object o)
    {
        if(!(o instanceof ColumnSlice))
            return false;
        ColumnSlice that = (ColumnSlice)o;
        return start.equals(that.start) && finish.equals(that.finish);
    }

    @Override
    public String toString()
    {
        return "[" + start + ", " + finish + "]";
    }

    public static class Serializer implements IVersionedSerializer<ColumnSlice>
    {
        private final CType type;

        public Serializer(CType type)
        {
            this.type = type;
        }

        public void serialize(ColumnSlice cs, DataOutput out, int version) throws IOException
        {
            ISerializer<Composite> serializer = type.serializer();
            serializer.serialize(cs.start, out);
            serializer.serialize(cs.finish, out);
        }

        public ColumnSlice deserialize(DataInput in, int version) throws IOException
        {
            ISerializer<Composite> serializer = type.serializer();
            Composite start = serializer.deserialize(in);
            Composite finish = serializer.deserialize(in);
            return new ColumnSlice(start, finish);
        }

        public long serializedSize(ColumnSlice cs, int version)
        {
            ISerializer<Composite> serializer = type.serializer();
            return serializer.serializedSize(cs.start, TypeSizes.NATIVE) + serializer.serializedSize(cs.finish, TypeSizes.NATIVE);
        }
    }

    public static class NavigableMapIterator extends AbstractIterator<Column>
    {
        private final NavigableMap<CellName, Column> map;
        private final ColumnSlice[] slices;

        private int idx = 0;
        private Iterator<Column> currentSlice;

        public NavigableMapIterator(NavigableMap<CellName, Column> map, ColumnSlice[] slices)
        {
            this.map = map;
            this.slices = slices;
        }

        protected Column computeNext()
        {
            if (currentSlice == null)
            {
                if (idx >= slices.length)
                    return endOfData();

                ColumnSlice slice = slices[idx++];
                // Note: we specialize the case of start == "" and finish = "" because it is slightly more efficient, but also they have a specific
                // meaning (namely, they always extend to the beginning/end of the range).
                if (slice.start.isEmpty())
                {
                    if (slice.finish.isEmpty())
                        currentSlice = map.values().iterator();
                    else
                        currentSlice = map.headMap(fakeCell(slice.finish), true).values().iterator();
                }
                else if (slice.finish.isEmpty())
                {
                    currentSlice = map.tailMap(fakeCell(slice.start), true).values().iterator();
                }
                else
                {
                    currentSlice = map.subMap(fakeCell(slice.start), true, fakeCell(slice.finish), true).values().iterator();
                }
            }

            if (currentSlice.hasNext())
                return currentSlice.next();

            currentSlice = null;
            return computeNext();
        }

        /*
         * We need to take a slice (headMap/tailMap/subMap) of a CellName map
         * based on a Composite. While CellName and Composite are comparable
         * and so this should work, I haven't found how to generify it properly.
         * So instead we create a "fake" CellName object that just encapsulate
         * the prefix. I might not be a valid CellName with respect to the CF
         * CellNameType, but this doesn't matter here (since we only care about
         * comparison). This is arguably a bit of a hack.
         */
        private static CellName fakeCell(Composite prefix)
        {
            return new AbstractCellName(prefix)
            {
                @Override
                public EOC eoc()
                {
                    return prefix.eoc();
                }

                public boolean isSameCQL3RowThan(CellName other)
                {
                    throw new UnsupportedOperationException();
                }

                public CellName copy(Allocator allocator)
                {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }
}
