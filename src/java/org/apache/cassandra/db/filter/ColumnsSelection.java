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
import java.io.IOException;
import java.util.*;
import java.security.MessageDigest;

import com.google.common.collect.Iterators;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.Cell;
import org.apache.cassandra.db.atoms.CellPath;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * This groups the name of selected columns for a query plus some optional
 * sub-selection for those columns (for querying subslices of a map for
 * instance)
 */
public class ColumnsSelection
{
    public static final Serializer serializer = new Serializer();

    private static final Comparator<ColumnIdentifier> keyComparator = new Comparator<ColumnIdentifier>()
    {
        public int compare(ColumnIdentifier id1, ColumnIdentifier id2)
        {
            return ByteBufferUtil.compareUnsigned(id1.bytes, id2.bytes);
        }
    };
    private static final Comparator<ColumnSubselection> valueComparator = new Comparator<ColumnSubselection>()
    {
        public int compare(ColumnSubselection s1, ColumnSubselection s2)
        {
            assert s1.column().name.equals(s2.column().name);
            return s1.column().cellPathComparator().compare(s1.minIncludedPath(), s2.minIncludedPath());
        }
    };

    private final PartitionColumns columns;
    private final SortedSetMultimap<ColumnIdentifier, ColumnSubselection> subSelections;

    private ColumnsSelection(PartitionColumns columns, SortedSetMultimap<ColumnIdentifier, ColumnSubselection> subSelections)
    {
        this.columns = columns;
        this.subSelections = subSelections;
    }

    public PartitionColumns columns()
    {
        return columns;
    }

    public boolean isEmpty()
    {
        return columns.isEmpty();
    }

    public static ColumnsSelection withoutSubselection(PartitionColumns columns)
    {
        return new ColumnsSelection(columns, null);
    }

    public boolean includes(Cell cell)
    {
        if (subSelections == null || !cell.column().isComplex())
            return true;

        SortedSet<ColumnSubselection> s = subSelections.get(cell.column().name);
        if (s.isEmpty())
            return true;

        for (ColumnSubselection subSel : s)
            if (subSel.includes(cell.path()))
                return true;

        return false;
    }

    public Tester newTester(ColumnDefinition column)
    {
        if (subSelections == null)
            return null;

        SortedSet<ColumnSubselection> s = subSelections.get(column.name);
        if (s.isEmpty())
            return null;

        return new Tester(s.iterator());
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Tester
    {
        private ColumnSubselection current;
        private final Iterator<ColumnSubselection> iterator;

        private Tester(Iterator<ColumnSubselection> iterator)
        {
            this.iterator = iterator;
        }

        public boolean includes(CellPath path)
        {
            while (current == null)
            {
                if (!iterator.hasNext())
                    return false;

                current = iterator.next();
                if (current.includes(path))
                    return true;

                if (current.column().cellPathComparator().compare(current.maxIncludedPath(), path) < 0)
                    current = null;
            }
            return false;
        }
    }

    public static class Builder
    {
        private final PartitionColumns.Builder columnsBuilder = PartitionColumns.builder();
        private final List<ColumnSubselection> selections = new ArrayList<>();

        public Builder add(ColumnDefinition c)
        {
            columnsBuilder.add(c);
            return this;
        }

        public Builder slice(ColumnDefinition c, CellPath from, CellPath to)
        {
            add(c);
            selections.add(ColumnSubselection.slice(c, from, to));
            return this;
        }

        public Builder select(ColumnDefinition c, CellPath elt)
        {
            add(c);
            selections.add(ColumnSubselection.elementSelection(c, elt));
            return this;
        }

        public ColumnsSelection build()
        {
            PartitionColumns columns = columnsBuilder.build();
            if (selections.isEmpty())
                return withoutSubselection(columns);

            SortedSetMultimap<ColumnIdentifier, ColumnSubselection> s = TreeMultimap.create(keyComparator, valueComparator);
            for (ColumnSubselection subSel : selections)
                s.put(subSel.column().name, subSel);
            return new ColumnsSelection(columns, s);
        }
    }

    @Override
    public String toString()
    {
        if (isEmpty())
            return "";

        Iterator<ColumnDefinition> defs = columns.selectOrderIterator();
        StringBuilder sb = new StringBuilder();
        appendColumnDef(sb, defs.next());
        while (defs.hasNext())
            appendColumnDef(sb.append(", "), defs.next());
        return sb.toString();
    }

    private void appendColumnDef(StringBuilder sb, ColumnDefinition column)
    {
        if (subSelections == null)
        {
            sb.append(column.name);
            return;
        }

        SortedSet<ColumnSubselection> s = subSelections.get(column.name);
        if (s.isEmpty())
        {
            sb.append(column.name);
            return;
        }

        int i = 0;
        for (ColumnSubselection subSel : s)
            sb.append(i++ == 0 ? "" : ", ").append(column.name).append(subSel);
    }

    public static class Serializer
    {
        public void serialize(ColumnsSelection selection, DataOutputPlus out, int version) throws IOException
        {
            Columns.serializer.serialize(selection.columns().statics, out);
            Columns.serializer.serialize(selection.columns().regulars, out);
            if (selection.subSelections == null)
            {
                out.writeShort(0);
            }
            else
            {
                out.writeShort(selection.subSelections.size());
                for (ColumnSubselection subSel : selection.subSelections.values())
                    ColumnSubselection.serializer.serialize(subSel, out, version);
            }
        }

        public ColumnsSelection deserialize(DataInput in, int version, CFMetaData metadata) throws IOException
        {
            Columns statics = Columns.serializer.deserialize(in, metadata);
            Columns regulars = Columns.serializer.deserialize(in, metadata);
            PartitionColumns columns = new PartitionColumns(statics, regulars);

            int nbSelections = in.readUnsignedShort();
            if (nbSelections == 0)
                return ColumnsSelection.withoutSubselection(columns);

            SortedSetMultimap<ColumnIdentifier, ColumnSubselection> selections = TreeMultimap.create(keyComparator, valueComparator);
            for (int i = 0; i < nbSelections; i++)
            {
                ColumnSubselection subSel = ColumnSubselection.serializer.deserialize(in, version, metadata);
                selections.put(subSel.column().name, subSel);
            }
            return new ColumnsSelection(columns, selections);
        }

        public long serializedSize(ColumnsSelection selection, int version, TypeSizes sizes)
        {
            long size = Columns.serializer.serializedSize(selection.columns().statics, sizes)
                      + Columns.serializer.serializedSize(selection.columns().regulars, sizes)
                      + sizes.sizeof((short)(selection.subSelections == null ? 0 : selection.subSelections.size()));

            if (selection.subSelections != null)
            {
                for (ColumnSubselection subSel : selection.subSelections.values())
                    size += ColumnSubselection.serializer.serializedSize(subSel, version, sizes);
            }
            return size;
        }
    }
}
