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
package org.apache.cassandra.thrift;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.partitions.*;

/**
 * Given an iterator on a partition of a compact table (a "static" compact or a super columns table), this
 * return an iterator that merges the static row columns with the other results.
 */
public class ThriftResultsMerger extends WrappingPartitionIterator
{
    private ThriftResultsMerger(PartitionIterator wrapped)
    {
        super(wrapped);
    }

    public static PartitionIterator maybeWrap(PartitionIterator iterator, CFMetaData metadata)
    {
        if (!metadata.isStaticCompactTable() && !metadata.isSuper())
            return iterator;

        return new ThriftResultsMerger(iterator);
    }

    public static AtomIterator maybeWrap(AtomIterator iterator)
    {
        if (!iterator.metadata().isStaticCompactTable() && !iterator.metadata().isSuper())
            return iterator;

        return iterator.metadata().isSuper()
             ? new SuperColumnsPartitionMerger(iterator)
             : new PartitionMerger(iterator);
    }

    protected AtomIterator computeNext(AtomIterator iter)
    {
        return iter.metadata().isSuper()
             ? new SuperColumnsPartitionMerger(iter)
             : new PartitionMerger(iter);
    }

    private static class PartitionMerger extends WrappingAtomIterator
    {
        // We initialize lazily to avoid having this iterator fetch the wrapped iterator before it's actually asked for it.
        private boolean isInit;

        private Row staticRow;
        private int i; // the index of the next column of static row to return

        private ReusableRow nextToMerge;
        private Atom nextFromWrapped;

        private PartitionMerger(AtomIterator results)
        {
            super(results);
            assert results.metadata().isStaticCompactTable();
        }

        private void init()
        {
            assert !isInit;
            this.staticRow = super.staticRow();
            assert staticRow.columns().complexColumnCount() == 0;

            this.nextToMerge = createReusableRow();
            updateNextToMerge();
            isInit = true;
        }

        @Override
        public Row staticRow()
        {
            return Rows.EMPTY_STATIC_ROW;
        }

        private ReusableRow createReusableRow()
        {
            return new ReusableRow(metadata().clusteringColumns().size(), metadata().partitionColumns().regulars, nowInSec(), metadata().isCounter());
        }

        @Override
        public boolean hasNext()
        {
            if (!isInit)
                init();

            return nextFromWrapped != null || nextToMerge != null || super.hasNext();
        }

        @Override
        public Atom next()
        {
            if (!isInit)
                init();

            if (nextFromWrapped == null && super.hasNext())
                nextFromWrapped = super.next();

            if (nextFromWrapped == null)
            {
                if (nextToMerge == null)
                    throw new NoSuchElementException();

                return consumeNextToMerge();
            }

            if (nextToMerge == null)
                return consumeNextWrapped();

            int cmp = metadata().comparator.compare(nextToMerge, nextFromWrapped);
            if (cmp < 0)
                return consumeNextToMerge();
            if (cmp > 0)
                return consumeNextWrapped();

            // Same row, but we know the row has only a single column so just pick the more recent
            assert nextFromWrapped instanceof Row;
            ReusableRow row = createReusableRow();
            Rows.merge((Row)consumeNextWrapped(), consumeNextToMerge(), columns().regulars, row.writer(), nowInSec());
            return row;
        }

        private Atom consumeNextWrapped()
        {
            Atom toReturn = nextFromWrapped;
            nextFromWrapped = null;
            return toReturn;
        }

        private Row consumeNextToMerge()
        {
            Row toReturn = nextToMerge;
            updateNextToMerge();
            return toReturn;
        }

        private void updateNextToMerge()
        {
            while (i < staticRow.columns().simpleColumnCount())
            {
                Cell cell = staticRow.getCell(staticRow.columns().getSimple(i++));
                if (cell != null)
                {
                    // Given a static cell, the equivalent row uses the column name as clustering and the
                    // value as unique cell value.
                    Row.Writer writer = nextToMerge.writer();
                    writer.writeClusteringValue(cell.column().name.bytes);
                    writer.writeCell(metadata().compactValueColumn(), cell.isCounterCell(), cell.value(), cell.livenessInfo(), cell.path());
                    writer.writeMaxLiveTimestamp(cell.livenessInfo().timestamp());
                    writer.endOfRow();
                    return;
                }
            }
            // Nothing more to merge.
            nextToMerge = null;
        }
    }

    private static class SuperColumnsPartitionMerger extends WrappingAtomIterator
    {
        private final ReusableRow reusableRow;
        private final ColumnDefinition superColumnMapColumn;
        private final AbstractType<?> columnComparator;

        private SuperColumnsPartitionMerger(AtomIterator results)
        {
            super(results);
            assert results.metadata().isSuper();

            this.superColumnMapColumn = results.metadata().compactValueColumn();
            assert superColumnMapColumn != null && superColumnMapColumn.type instanceof MapType;

            this.reusableRow = new ReusableRow(results.metadata().clusteringColumns().size(),
                                               Columns.of(superColumnMapColumn),
                                               results.nowInSec(),
                                               results.metadata().isCounter());
            this.columnComparator = ((MapType)superColumnMapColumn.type).nameComparator();
        }

        @Override
        public Atom next()
        {
            Atom next = super.next();
            if (next.kind() != Atom.Kind.ROW)
                return next;

            Row row = (Row)next;
            Row.Writer writer = reusableRow.writer();
            row.clustering().writeTo(writer);

            PeekingIterator<Cell> staticCells = Iterators.peekingIterator(makeStaticCellIterator(row));
            if (!staticCells.hasNext())
                return row;

            Iterator<Cell> cells = row.getCells(superColumnMapColumn);
            PeekingIterator<Cell> dynamicCells = Iterators.peekingIterator(cells.hasNext() ? cells : Collections.<Cell>emptyIterator());

            while (staticCells.hasNext() && dynamicCells.hasNext())
            {
                Cell staticCell = staticCells.peek();
                Cell dynamicCell = dynamicCells.peek();
                int cmp = columnComparator.compare(staticCell.column().name.bytes, dynamicCell.path().get(0));
                if (cmp < 0)
                {
                    staticCell = staticCells.next();
                    writer.writeCell(superColumnMapColumn, staticCell.isCounterCell(), staticCell.value(), staticCell.livenessInfo(), CellPath.create(staticCell.column().name.bytes));
                }
                else if (cmp > 0)
                {
                    dynamicCells.next().writeTo(writer);
                }
                else
                {
                    staticCell = staticCells.next();
                    Cell toMerge = Cells.create(superColumnMapColumn,
                                                 staticCell.isCounterCell(),
                                                 staticCell.value(),
                                                 staticCell.livenessInfo(),
                                                 CellPath.create(staticCell.column().name.bytes));
                    Cells.reconcile(toMerge, dynamicCells.next(), nowInSec()).writeTo(writer);
                }
            }

            while (staticCells.hasNext())
            {
                Cell staticCell = staticCells.next();
                writer.writeCell(superColumnMapColumn, staticCell.isCounterCell(), staticCell.value(), staticCell.livenessInfo(), CellPath.create(staticCell.column().name.bytes));
            }
            while (dynamicCells.hasNext())
            {
                dynamicCells.next().writeTo(writer);
            }

            writer.endOfRow();
            return reusableRow;
        }

        private static Iterator<Cell> makeStaticCellIterator(final Row row)
        {
            return new AbstractIterator<Cell>()
            {
                private int i;

                protected Cell computeNext()
                {
                    while (i < row.columns().simpleColumnCount())
                    {
                        Cell cell = row.getCell(row.columns().getSimple(i++));
                        if (cell != null)
                            return cell;
                    }
                    return endOfData();
                }
            };
        }
    }
}

