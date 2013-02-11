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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.cql3.CQL3Row;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.ColumnToCollectionType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.IVersionedSerializer;

import org.apache.cassandra.utils.ByteBufferUtil;

public class SparseCellNameType extends AbstractCellNameType
{
    protected final CType type;

    SparseCellNameType(CType clusteringType)
    {
        assert !clusteringType.isPacked() || clusteringType.size() == 0;
        this.type = clusteringType;
    }

    public boolean isPacked()
    {
        return type.isPacked();
    }

    public int size()
    {
        return type.size() + 1;
    }

    public AbstractType<?> subtype(int i)
    {
        if (i >= size())
            throw new IndexOutOfBoundsException();

        return i == type.size()
             ? UTF8Type.instance
             : type.subtype(i);
    }

    public CBuilder builder()
    {
        if (isPacked())
        {
            return new SimpleCType(UTF8Type.instance).builder();
        }
        else
        {
            return new CompositeCType(allSubtypes()).builder();
        }
    }

    private List<AbstractType<?>> allSubtypes()
    {
        List<AbstractType<?>> allSubtypes = new ArrayList<AbstractType<?>>(size());
        for (int i = 0; i < size(); i++)
            allSubtypes.add(subtype(i));
        return allSubtypes;
    }

    public CellNameType setSubtype(int position, AbstractType<?> newType)
    {
        if (position < type.size())
            return new SparseCellNameType(type.setSubtype(position, newType));

        if (position == type.size())
            throw new IllegalArgumentException();

        throw new IndexOutOfBoundsException();
    }

    public CellNameType addCollection(ByteBuffer columnName, CollectionType newCollection)
    {
        return new WithCollection(type, ColumnToCollectionType.getInstance(Collections.singletonMap(columnName, newCollection)));
    }

    public AbstractType<?> asAbstractType()
    {
        return isPacked()
             ? UTF8Type.instance
             : CompositeType.getInstance(allSubtypes());
    }

    public boolean isDense()
    {
        return false;
    }

    public Composite fromByteBuffer(ByteBuffer bb)
    {
        if (bb.remaining() == 0)
            return Composites.EMPTY;

        if (isPacked())
            return new SparseCellName(Composites.EMPTY, bb);

        List<ByteBuffer> elements = new ArrayList<ByteBuffer>(size());
        Composite.EOC eoc = readCompositeElements(bb, elements);
        if (elements.size() <= type.size() || eoc != Composite.EOC.NONE)
            return new ListComposite(elements).withEOC(eoc);

        return makeCellName(elements);
    }

    protected CellName makeCellName(List<ByteBuffer> components)
    {
        if (isPacked())
            return new SparseCellName(Composites.EMPTY, components.get(0));

        List<ByteBuffer> ck = new ArrayList<ByteBuffer>(components.size() - 1);
        for (int i = 0; i < components.size() - 1; i++)
            ck.add(components.get(i));
        return new SparseCellName(new ListComposite(ck), components.get(components.size() - 1));
    }

    @Override
    public boolean supportCollections()
    {
        return true;
    }

    public CQL3Row.Builder CQL3RowBuilder()
    {
        return new CQL3Row.Builder()
        {
            public Iterator<CQL3Row> group(final Iterator<Column> cells)
            {
                return new AbstractIterator<CQL3Row>()
                {
                    private CellName previous;
                    private CQL3RowOfSparse currentRow;

                    protected CQL3Row computeNext()
                    {
                        while (cells.hasNext())
                        {
                            final Column cell = cells.next();
                            if (cell.isMarkedForDelete())
                                continue;

                            CQL3Row toReturn = null;
                            CellName current = cell.name();
                            if (currentRow == null || !current.isSameCQL3RowThan(previous))
                            {
                                toReturn = currentRow;
                                currentRow = new CQL3RowOfSparse(current.clusteringPrefix());
                            }
                            currentRow.add(cell);
                            previous = current;

                            if (toReturn != null)
                                return toReturn;
                        }
                        if (currentRow != null)
                        {
                            CQL3Row toReturn = currentRow;
                            currentRow = null;
                            return toReturn;
                        }
                        return endOfData();
                    }
                };
            }
        };
    }

    private static class CQL3RowOfSparse implements CQL3Row
    {
        private final Composite clusteringKey;
        private final Map<ByteBuffer, Column> columns = new HashMap<ByteBuffer, Column>();
        private final Map<ByteBuffer, List<Column>> collections = new HashMap<ByteBuffer, List<Column>>();

        CQL3RowOfSparse(Composite clusteringKey)
        {
            this.clusteringKey = clusteringKey;
        }

        public Composite clusteringKey()
        {
            return clusteringKey;
        }

        void add(Column cell)
        {
            CellName cellName = cell.name();
            ByteBuffer columnName =  cellName.cql3ColumnName();
            if (cellName.isCollectionCell())
            {
                List<Column> values = collections.get(columnName);
                if (values == null)
                {
                    values = new ArrayList<Column>();
                    collections.put(columnName, values);
                }
                values.add(cell);
            }
            else
            {
                columns.put(columnName, cell);
            }
        }

        public Column getColumn(ByteBuffer name)
        {
            return columns.get(name);
        }

        public List<Column> getCollection(ByteBuffer name)
        {
            return collections.get(name);
        }
    }

    static class WithCollection extends SparseCellNameType
    {
        private final ColumnToCollectionType collectionType;

        WithCollection(CType clusteringType, ColumnToCollectionType collectionType)
        {
            super(clusteringType);
            assert !clusteringType.isPacked() && collectionType != null;
            this.collectionType = collectionType;
        }

        @Override
        public int size()
        {
            return super.size() + 1;
        }

        @Override
        public AbstractType<?> subtype(int i)
        {
            if (i >= size())
                throw new IndexOutOfBoundsException();

            return i == size() - 1
                 ? collectionType
                 : super.subtype(i);
        }

        @Override
        public CellNameType setSubtype(int position, AbstractType<?> newType)
        {
            if (position < type.size() - 1)
                return new SparseCellNameType.WithCollection(type.setSubtype(position, newType), collectionType);

            throw position >= type.size() ? new IndexOutOfBoundsException() : new IllegalArgumentException();
        }

        @Override
        public CellNameType addCollection(ByteBuffer columnName, CollectionType newCollection)
        {
            Map<ByteBuffer, CollectionType> newMap = new HashMap<ByteBuffer, CollectionType>(collectionType.defined);
            newMap.put(columnName, newCollection);
            return new WithCollection(type, ColumnToCollectionType.getInstance(newMap));
        }

        @Override
        protected CellName makeCellName(List<ByteBuffer> components)
        {
            int csize = clusteringPrefixSize();
            List<ByteBuffer> ck = new ArrayList<ByteBuffer>(csize);
            for (int i = 0; i < csize; i++)
                ck.add(components.get(i));
            ByteBuffer columnName = components.get(csize);
            if (components.size() == csize + 1)
            {
                return new SparseCellName(new ListComposite(ck), columnName);
            }
            else
            {
                assert components.size() == csize + 2;
                return new SparseCellName.WithCollectionElement(new ListComposite(ck), columnName, components.get(csize + 1));
            }
        }

        @Override
        public boolean hasCollections()
        {
            return true;
        }

        @Override
        public ColumnToCollectionType collectionType()
        {
            return collectionType;
        }
    }
}
