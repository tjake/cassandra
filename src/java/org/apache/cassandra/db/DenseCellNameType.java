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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.cql3.CQL3Row;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ColumnToCollectionType;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.IVersionedSerializer;

public class DenseCellNameType extends AbstractCellNameType
{
    private final CType type;

    public DenseCellNameType(CType type)
    {
        this.type = type;
    }

    public boolean isPacked()
    {
        return type.isPacked();
    }

    public int size()
    {
        return type.size();
    }

    public AbstractType<?> subtype(int i)
    {
        return type.subtype(i);
    }

    public CBuilder builder()
    {
        return type.builder();
    }

    public CellNameType setSubtype(int position, AbstractType<?> newType)
    {
        return new DenseCellNameType(type.setSubtype(position, newType));
    }

    public AbstractType<?> asAbstractType()
    {
        return type.asAbstractType();
    }

    public boolean isDense()
    {
        return true;
    }

    public Composite fromByteBuffer(ByteBuffer bb)
    {
        if (bb.remaining() == 0)
            return Composites.EMPTY;

        Composite c = type.fromByteBuffer(bb);
        return c.size() == size() ? new DenseCellName(c) : c;
    }

    protected CellName makeCellName(List<ByteBuffer> components)
    {
        return new DenseCellName(type.isPacked() ? new SimpleComposite(components.get(0)) : new ListComposite(components));
    }

    public CQL3Row.Builder CQL3RowBuilder()
    {
        return new CQL3Row.Builder()
        {
            public Iterator<CQL3Row> group(final Iterator<Column> cells)
            {
                return new AbstractIterator<CQL3Row>()
                {
                    protected CQL3Row computeNext()
                    {
                        while (cells.hasNext())
                        {
                            final Column cell = cells.next();
                            if (cell.isMarkedForDelete())
                                continue;

                            return new CQL3Row()
                            {
                                public Composite clusteringKey()
                                {
                                    return cell.name();
                                }

                                public Column getColumn(ByteBuffer name)
                                {
                                    return cell;
                                }

                                public List<Column> getCollection(ByteBuffer name)
                                {
                                    return null;
                                }
                            };
                        }
                        return endOfData();
                    }
                };
            }
        };
    }
}
