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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.ISSTableSerializer;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.thrift.ThriftConversion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.io.sstable.IndexHelper.IndexInfo;

/**
 * Represents the legacy layouts: dense/sparse and simple/compound.
 *
 * This is only use to serialize/deserialize the old format.
 */
public abstract class LegacyLayout
{
    public final static int MAX_CELL_NAME_LENGTH = FBUtilities.MAX_UNSIGNED_SHORT;

    public final static int DELETION_MASK        = 0x01;
    public final static int EXPIRATION_MASK      = 0x02;
    public final static int COUNTER_MASK         = 0x04;
    public final static int COUNTER_UPDATE_MASK  = 0x08;
    public final static int RANGE_TOMBSTONE_MASK = 0x10;

    private LegacyLayout() {}

    public static AbstractType<?> makeLegacyComparator(CFMetaData metadata)
    {
        ClusteringComparator comparator = metadata.comparator;
        if (!metadata.isCompound())
        {
            assert comparator.size() == 1;
            return comparator.subtype(0);
        }

        boolean hasCollections = metadata.hasCollectionColumns();
        List<AbstractType<?>> types = new ArrayList<>(comparator.size() + (metadata.isDense() ? 0 : 1) + (hasCollections ? 1 : 0));

        types.addAll(comparator.subtypes());

        if (!metadata.isDense())
        {
            types.add(UTF8Type.instance);
            if (hasCollections)
            {
                Map<ByteBuffer, CollectionType> defined = new HashMap<>();
                for (ColumnDefinition def : metadata.partitionColumns())
                {
                    if (def.type instanceof CollectionType && def.type.isMultiCell())
                        defined.put(def.name.bytes, (CollectionType)def.type);
                }
                types.add(ColumnToCollectionType.getInstance(defined));
            }
        }
        return CompositeType.getInstance(types);
    }

    public static LegacyCellName decodeCellName(CFMetaData metadata, ByteBuffer superColumnName, ByteBuffer cellname)
    {
        assert cellname != null;
        if (metadata.isSuper())
        {
            assert superColumnName != null;
            return decodeForSuperColumn(metadata, new SimpleClustering(superColumnName), cellname);
        }

        assert superColumnName == null;
        return decodeCellName(metadata, cellname);
    }

    private static LegacyCellName decodeForSuperColumn(CFMetaData metadata, Clustering clustering, ByteBuffer subcol)
    {
        ColumnDefinition def = metadata.getColumnDefinition(subcol);
        if (def != null)
        {
            // it's a statically defined subcolumn
            return new LegacyCellName(clustering, def, null);
        }

        def = metadata.compactValueColumn();
        assert def != null && def.type instanceof MapType;
        return new LegacyCellName(clustering, def, subcol);
    }

    public static LegacyCellName decodeCellName(CFMetaData metadata, ByteBuffer cellname)
    {
        Clustering clustering = decodeClustering(metadata, cellname);

        if (metadata.isSuper())
            return decodeForSuperColumn(metadata, clustering, CompositeType.extractComponent(cellname, 1));

        if (metadata.isDense())
            return new LegacyCellName(clustering, metadata.compactValueColumn(), null);

        ByteBuffer column = metadata.isCompound() ? CompositeType.extractComponent(cellname, metadata.comparator.size()) : cellname;
        if (column == null)
            throw new IllegalArgumentException("No column name component found in cell name");

        // Row marker, this is ok
        if (!column.hasRemaining())
            return new LegacyCellName(clustering, null, null);

        ColumnDefinition def = metadata.getColumnDefinition(column);
        if (def == null)
        {
            // If it's a compact table, it means the column is in fact a "dynamic" one
            if (metadata.isCompactTable())
                return new LegacyCellName(new SimpleClustering(column), metadata.compactValueColumn(), null);

            throw new IllegalArgumentException("Unknown column " + UTF8Type.instance.getString(column));
        }

        ByteBuffer collectionElement = metadata.isCompound() ? CompositeType.extractComponent(cellname, metadata.comparator.size() + 1) : null;
        return new LegacyCellName(def.isStatic() ? Clustering.STATIC_CLUSTERING : clustering, def, collectionElement);
    }

    public static Slice.Bound decodeBound(CFMetaData metadata, ByteBuffer bound, boolean isStart)
    {
        if (!bound.hasRemaining())
            return isStart ? Slice.Bound.BOTTOM : Slice.Bound.TOP;

        List<ByteBuffer> components = metadata.isCompound()
                                    ? CompositeType.splitName(bound)
                                    : Collections.singletonList(bound);

        assert components.size() <= metadata.comparator.size();

        return Slice.Bound.create(isStart ? Slice.Bound.Kind.INCL_START_BOUND : Slice.Bound.Kind.INCL_END_BOUND,
                                  components.toArray(new ByteBuffer[components.size()]));
    }

    public static ByteBuffer encodeCellName(CFMetaData metadata, Clustering clustering, ByteBuffer columnName, ByteBuffer collectionElement)
    {
        boolean isStatic = clustering == Clustering.STATIC_CLUSTERING;

        if (!metadata.isCompound())
        {
            if (isStatic)
                return columnName;

            assert clustering.size() == 1;
            return clustering.get(0);
        }

        // We use comparator.size() rather than clustering.size() because of static clusterings
        int clusteringSize = metadata.comparator.size();
        int size = clusteringSize + (metadata.isDense() ? 0 : 1) + (collectionElement == null ? 0 : 1);
        ByteBuffer[] values = new ByteBuffer[size];
        for (int i = 0; i < clusteringSize; i++)
        {
            if (isStatic)
            {
                values[i] = ByteBufferUtil.EMPTY_BYTE_BUFFER;
                continue;
            }

            ByteBuffer v = clustering.get(i);
            // we can have null (only for dense compound tables for backward compatibility reasons) but that
            // means we're done and should stop there as far as building the composite is concerned.
            if (v == null)
                return CompositeType.build(Arrays.copyOfRange(values, 0, i));

            values[i] = v;
        }

        if (!metadata.isDense())
            values[clusteringSize] = columnName;
        if (collectionElement != null)
            values[clusteringSize + 1] = collectionElement;

        return CompositeType.build(isStatic, values);
    }

    public static Clustering decodeClustering(CFMetaData metadata, ByteBuffer value)
    {
        int csize = metadata.comparator.size();
        if (csize == 0)
            return Clustering.EMPTY;

        List<ByteBuffer> components = metadata.isCompound()
                                    ? CompositeType.splitName(value)
                                    : Collections.singletonList(value);

        return new SimpleClustering(components.subList(0, Math.min(csize, components.size())).toArray(new ByteBuffer[csize]));
    }

    public static ByteBuffer encodeClustering(Clustering clustering)
    {
        ByteBuffer[] values = new ByteBuffer[clustering.size()];
        for (int i = 0; i < clustering.size(); i++)
            values[i] = clustering.get(i);
        return CompositeType.build(values);
    }

    // For serializing to old wire format
    public static Iterator<LegacyAtom> fromAtomIterator(AtomIterator iterator)
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    // For deserializing old wire format/old sstables
    public static AtomIterator toAtomIterator(CFMetaData metadata,
                                              DecoratedKey key,
                                              DeletionTime partitionDeletion,
                                              Iterator<LegacyAtom> atoms,
                                              boolean reversed,
                                              int nowInSec)
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public static Iterator<LegacyCell> fromRowIterator(final RowIterator iterator)
    {
        return new AbstractIterator<LegacyCell>()
        {
            private Iterator<LegacyCell> currentRow = iterator.staticRow().isEmpty()
                                                    ? Collections.<LegacyLayout.LegacyCell>emptyIterator()
                                                    : fromRow(iterator.metadata(), iterator.staticRow());

            protected LegacyCell computeNext()
            {
                if (currentRow.hasNext())
                    return currentRow.next();

                if (!iterator.hasNext())
                    return endOfData();

                currentRow = fromRow(iterator.metadata(), iterator.next());
                return computeNext();
            }
        };
    }

    private static Iterator<LegacyCell> fromRow(final CFMetaData metadata, final Row row)
    {
        return new AbstractIterator<LegacyCell>()
        {
            private final Iterator<Cell> cells = row.iterator();
            // we don't have (and shouldn't have) row markers for compact tables.
            private boolean hasReturnedRowMarker = metadata.isCompactTable();

            protected LegacyCell computeNext()
            {
                if (!hasReturnedRowMarker)
                {
                    hasReturnedRowMarker = true;
                    LegacyCellName cellName = new LegacyCellName(row.clustering(), null, null);
                    LivenessInfo info = row.partitionKeyLivenessInfo();
                    return new LegacyCell(LegacyCell.Kind.REGULAR, cellName, ByteBufferUtil.EMPTY_BYTE_BUFFER, info.timestamp(), info.localDeletionTime(), info.ttl());
                }

                if (!cells.hasNext())
                    return endOfData();

                Cell cell = cells.next();
                return makeLegacyCell(metadata, row.clustering(), cell);
            }
        };
    }

    private static LegacyCell makeLegacyCell(CFMetaData metadata, Clustering clustering, Cell cell)
    {
        LegacyCell.Kind kind;
        if (cell.isCounterCell())
            kind = LegacyCell.Kind.COUNTER;
        else if (cell.isTombstone())
            kind = LegacyCell.Kind.DELETED;
        else if (cell.isExpiring())
            kind = LegacyCell.Kind.EXPIRING;
        else
            kind = LegacyCell.Kind.REGULAR;

        CellPath path = cell.path();
        assert path == null || path.size() == 1;
        LegacyCellName name = new LegacyCellName(clustering, cell.column(), path == null ? null : path.get(0));
        LivenessInfo info = cell.livenessInfo();
        return new LegacyCell(kind, name, cell.value(), info.timestamp(), info.localDeletionTime(), info.ttl());
    }

    public static RowIterator toRowIterator(final CFMetaData metadata,
                                            final DecoratedKey key,
                                            final Iterator<LegacyCell> cells,
                                            final int nowInSec)
    {
        return new RowIterator()
        {
            private final ReusableRow row = new ReusableRow(metadata.clusteringColumns().size(), metadata.partitionColumns().regulars, nowInSec);
            private LegacyCell nextCell;

            public CFMetaData metadata()
            {
                return metadata;
            }

            public boolean isReverseOrder()
            {
                return false;
            }

            public PartitionColumns columns()
            {
                return metadata.partitionColumns();
            }

            public DecoratedKey partitionKey()
            {
                return key;
            }

            public Row staticRow()
            {
                return Rows.EMPTY_STATIC_ROW;
            }

            public int nowInSec()
            {
                return nowInSec;
            }

            public boolean hasNext()
            {
                return nextCell != null || cells.hasNext();
            }

            public Row next()
            {
                // Use the next available cell to set the clustering
                if (nextCell == null)
                    nextCell = cells.next();

                Clustering clustering = nextCell.name.clustering.takeAlias();

                Row.Writer writer = row.writer();
                clustering.writeTo(writer);

                if (nextCell.name.column == null)
                {
                    // It's the row marker
                    assert !nextCell.value.hasRemaining();
                    writer.writePartitionKeyLivenessInfo(livenessInfo(metadata, nextCell));
                    nextCell = cells.hasNext() ? cells.next() : null;
                }

                while (nextCell != null && clustering.equals(nextCell.name.clustering))
                {
                    assert nextCell.name.column != null; // we've already handled the row marker
                    CellPath path = nextCell.name.collectionElement == null ? null : CellPath.create(nextCell.name.collectionElement);
                    writer.writeCell(nextCell.name.column, nextCell.isCounter(), nextCell.value, livenessInfo(metadata, nextCell), path);
                    nextCell = cells.hasNext() ? cells.next() : null;
                }

                writer.endOfRow();
                return row;
            }

            public void remove()
            {
                throw new UnsupportedOperationException();
            }

            public void close()
            {
            }
        };
    }

    private static LivenessInfo livenessInfo(CFMetaData metadata, LegacyCell cell)
    {
        return cell.isTombstone()
             ? SimpleLivenessInfo.forDeletion(cell.timestamp, cell.localDeletionTime)
             : SimpleLivenessInfo.forUpdate(cell.timestamp, cell.ttl, cell.localDeletionTime, metadata);
    }

    public static Comparator<LegacyCell> legacyCellComparator(final CFMetaData metadata)
    {
        return new Comparator<LegacyCell>()
        {
            public int compare(LegacyCell cell1, LegacyCell cell2)
            {
                LegacyCellName c1 = cell1.name;
                LegacyCellName c2 = cell2.name;

                // Compare clustering first
                int c = metadata.comparator.compare(c1.clustering, c2.clustering);
                if (c != 0)
                    return 0;

                // Then check the column name
                if (c1.column != c2.column)
                {
                    // A null for the column means it's a row marker
                    if (c1.column == null)
                        return c2.column == null ? 0 : -1;
                    if (c2.column == null)
                        return 1;

                    assert c1.column.isRegular() || c1.column.isStatic();
                    assert c2.column.isRegular() || c2.column.isStatic();
                    if (c1.column.kind != c2.column.kind)
                        return c1.column.isStatic() ? -1 : 1;

                    AbstractType<?> cmp = metadata.getColumnDefinitionNameComparator(c1.column.kind);
                    c = cmp.compare(c1.column.name.bytes, c2.column.name.bytes);
                    if (c != 0)
                        return c;
                }

                assert (c1.collectionElement == null) == (c2.collectionElement == null);

                if (c1.collectionElement != null)
                {
                    AbstractType<?> colCmp = ((CollectionType)c1.column.type).nameComparator();
                    return colCmp.compare(c1.collectionElement, c2.collectionElement);
                }
                return 0;
            }
        };
    }

    public static class LegacyCellName
    {
        public final Clustering clustering;
        public final ColumnDefinition column;
        public final ByteBuffer collectionElement;

        private LegacyCellName(Clustering clustering, ColumnDefinition column, ByteBuffer collectionElement)
        {
            this.clustering = clustering;
            this.column = column;
            this.collectionElement = collectionElement;
        }

        public ByteBuffer encode(CFMetaData metadata)
        {
            return encodeCellName(metadata, clustering, column == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : column.name.bytes, collectionElement);
        }

        public ByteBuffer superColumnSubName()
        {
            assert collectionElement != null;
            return collectionElement;
        }

        public ByteBuffer superColumnName()
        {
            return clustering.get(0);
        }
    }

    public interface LegacyAtom
    {
        public boolean isCell();

        public LegacyCell asCell();
        public LegacyRangeTombstone asRangeTombstone();
    }

    /**
     * A legacy cell.
     * <p>
     * This is used as a temporary object to facilitate dealing with the legacy format, this
     * is not meant to be optimal.
     */
    public static class LegacyCell implements LegacyAtom
    {
        public enum Kind { REGULAR, EXPIRING, DELETED, COUNTER }

        public final Kind kind;

        public final LegacyCellName name;
        public final ByteBuffer value;

        public final long timestamp;
        public final int localDeletionTime;
        public final int ttl;

        private LegacyCell(Kind kind, LegacyCellName name, ByteBuffer value, long timestamp, int localDeletionTime, int ttl)
        {
            this.kind = kind;
            this.name = name;
            this.value = value;
            this.timestamp = timestamp;
            this.localDeletionTime = localDeletionTime;
            this.ttl = ttl;
        }

        public static LegacyCell regular(CFMetaData metadata, ByteBuffer superColumnName, ByteBuffer name, ByteBuffer value, long timestamp)
        {
            return new LegacyCell(Kind.REGULAR, decodeCellName(metadata, superColumnName, name), value, timestamp, LivenessInfo.NO_DELETION_TIME, LivenessInfo.NO_TTL);
        }

        public static LegacyCell expiring(CFMetaData metadata, ByteBuffer superColumnName, ByteBuffer name, ByteBuffer value, long timestamp, int ttl, int nowInSec)
        {
            return new LegacyCell(Kind.EXPIRING, decodeCellName(metadata, superColumnName, name), value, timestamp, nowInSec, ttl);
        }

        public static LegacyCell tombstone(CFMetaData metadata, ByteBuffer superColumnName, ByteBuffer name, long timestamp, int nowInSec)
        {
            return new LegacyCell(Kind.DELETED, decodeCellName(metadata, superColumnName, name), ByteBufferUtil.EMPTY_BYTE_BUFFER, timestamp, nowInSec, LivenessInfo.NO_TTL);
        }

        public static LegacyCell counter(CFMetaData metadata, ByteBuffer superColumnName, ByteBuffer name, long value)
        {
            return new LegacyCell(Kind.COUNTER, decodeCellName(metadata, superColumnName, name), ByteBufferUtil.bytes(value), FBUtilities.timestampMicros(), LivenessInfo.NO_DELETION_TIME, LivenessInfo.NO_TTL);
        }

        public boolean isCell()
        {
            return true;
        }

        public LegacyCell asCell()
        {
            return this;
        }

        public LegacyRangeTombstone asRangeTombstone()
        {
            throw new UnsupportedOperationException();
        }

        public boolean isCounter()
        {
            return kind == Kind.COUNTER;
        }

        public boolean isExpiring()
        {
            return kind == Kind.EXPIRING;
        }

        public boolean isTombstone()
        {
            return kind == Kind.DELETED;
        }
    }

    /**
     * A legacy range tombstone.
     * <p>
     * This is used as a temporary object to facilitate dealing with the legacy format, this
     * is not meant to be optimal.
     */
    public static class LegacyRangeTombstone
    {
        public final ByteBuffer start;
        public final ByteBuffer stop;
        public final DeletionTime deletionTime;

        private LegacyRangeTombstone(ByteBuffer start, ByteBuffer stop, DeletionTime deletionTime)
        {
            this.start = start;
            this.stop = stop;
            this.deletionTime = deletionTime;
        }

        public boolean isCell()
        {
            return false;
        }

        public LegacyCell asCell()
        {
            throw new UnsupportedOperationException();
        }

        public LegacyRangeTombstone asRangeTombstone()
        {
            return this;
        }
    }

    public static class LegacyPartition
    {
        public List<LegacyRangeTombstone> rangeTombstones;
        public Map<ByteBuffer, LegacyCell> cells;
    }

    public static class DecodedCellName
    {
        public final Clustering clustering;
        public final ColumnDefinition column;
        public final ByteBuffer collectionElement;

        private DecodedCellName(Clustering clustering, ColumnDefinition column, ByteBuffer collectionElement)
        {
            this.clustering = clustering;
            this.column = column;
            this.collectionElement = collectionElement;
        }
    }

    //public void deserializeCellBody(DataInput in, DeserializedCell cell, ByteBuffer collectionElement, int mask, Flag flag, int expireBefore)
    //throws IOException
    //{
    //    if ((mask & COUNTER_MASK) != 0)
    //    {
    //        // TODO
    //        throw new UnsupportedOperationException();
    //        ///long timestampOfLastDelete = in.readLong();
    //        ///long ts = in.readLong();
    //        ///ByteBuffer value = ByteBufferUtil.readWithLength(in);
    //        ///return BufferCounterCell.create(name, value, ts, timestampOfLastDelete, flag);
    //    }
    //    else if ((mask & COUNTER_UPDATE_MASK) != 0)
    //    {
    //        // TODO
    //        throw new UnsupportedOperationException();
    //    }
    //    else if ((mask & EXPIRATION_MASK) != 0)
    //    {
    //        assert collectionElement == null;
    //        cell.isCounter = false;
    //        cell.ttl = in.readInt();
    //        cell.localDeletionTime = in.readInt();
    //        cell.timestamp = in.readLong();
    //        cell.value = ByteBufferUtil.readWithLength(in);
    //        cell.path = null;

    //        // Transform expired cell to tombstone (as it basically saves the value)
    //        if (cell.localDeletionTime < expireBefore && flag != Flag.PRESERVE_SIZE)
    //        {
    //            // The column is now expired, we can safely return a simple tombstone. Note that
    //            // as long as the expiring column and the tombstone put together live longer than GC grace seconds,
    //            // we'll fulfil our responsibility to repair.  See discussion at
    //            // http://cassandra-user-incubator-apache-org.3065146.n2.nabble.com/repair-compaction-and-tombstone-rows-td7583481.html
    //            cell.localDeletionTime = cell.localDeletionTime - cell.ttl;
    //            cell.ttl = 0;
    //            cell.value = null;
    //        }

    //    }
    //    else
    //    {
    //        cell.isCounter = false;
    //        cell.ttl = 0;
    //        cell.path = null;

    //        cell.timestamp = in.readLong();
    //        ByteBuffer value = ByteBufferUtil.readWithLength(in);

    //        boolean isDeleted = (mask & DELETION_MASK) != 0;
    //        cell.value = isDeleted ? null : value;
    //        cell.localDeletionTime = isDeleted ? ByteBufferUtil.toInt(value) : Integer.MAX_VALUE;
    //    }
    //}

    //public void skipCellBody(DataInput in, int mask)
    //throws IOException
    //{
    //    if ((mask & COUNTER_MASK) != 0)
    //        FileUtils.skipBytesFully(in, 16);
    //    else if ((mask & EXPIRATION_MASK) != 0)
    //        FileUtils.skipBytesFully(in, 16);
    //    else
    //        FileUtils.skipBytesFully(in, 8);

    //    int length = in.readInt();
    //    FileUtils.skipBytesFully(in, length);
    //}

    //public abstract IVersionedSerializer<ColumnSlice> sliceSerializer();
    //public abstract IVersionedSerializer<SliceQueryFilter> sliceQueryFilterSerializer();

    //public DeletionInfo.Serializer deletionInfoSerializer()
    //{
    //    return deletionInfoSerializer;
    //}

    //public RangeTombstone.Serializer rangeTombstoneSerializer()
    //{
    //    return rangeTombstoneSerializer;
    //}

    //public static class LegacyAtomDeserializer extends AtomDeserializer
    //{
    //    private final Deserializer nameDeserializer;

    //    private RangeTombstone openTombstone;

    //    private final ReusableRangeTombstoneMarker marker;
    //    private final ReusableRow row;

    //    private LegacyLayout.DeserializedCell cell;

    //    public LegacyAtomDeserializer(CFMetaData metadata,
    //                                  DataInput in,
    //                                  LegacyLayout.Flag flag,
    //                                  int expireBefore,
    //                                  Version version,
    //                                  Columns columns)
    //    {
    //        super(metadata, in, flag, expireBefore, version, columns);
    //        this.nameDeserializer = metadata.layout().newDeserializer(in, version);
    //        this.marker = new ReusableRangeTombstoneMarker();
    //        this.row = new ReusableRow(columns);
    //    }

    //    public boolean hasNext() throws IOException
    //    {
    //        return hasUnprocessed() || nameDeserializer.hasNext();
    //    }

    //    public boolean hasUnprocessed() throws IOException
    //    {
    //        return openTombstone != null || nameDeserializer.hasUnprocessed();
    //    }

    //    public int compareNextTo(Clusterable prefix) throws IOException
    //    {
    //        if (openTombstone != null && nameDeserializer.compareNextTo(openTombstone.max) > 0)
    //            return metadata.comparator.compare(openTombstone.max, prefix);

    //        return nameDeserializer.compareNextTo(prefix);
    //    }

    //    public Atom readNext() throws IOException
    //    {
    //        if (openTombstone != null && nameDeserializer.compareNextTo(openTombstone.max) > 0)
    //            return marker.setTo(openTombstone.max, false, openTombstone.data);

    //        Clustering prefix = nameDeserializer.readNextClustering();
    //        int b = in.readUnsignedByte();
    //        if ((b & LegacyLayout.RANGE_TOMBSTONE_MASK) != 0)
    //        {
    //            // TODO: deal with new style RT
    //            openTombstone = metadata.layout().rangeTombstoneSerializer().deserializeBody(in, prefix, version);
    //            return marker.setTo(openTombstone.min, true, openTombstone.data);
    //        }

    //        Row.Writer writer = row.writer();
    //        writer.writeClustering(prefix);

    //        // If there is a row marker, it's the first cell
    //        ByteBuffer columnName = nameDeserializer.getNextColumnName();
    //        if (columnName != null && !columnName.hasRemaining())
    //        {
    //            metadata.layout().deserializeCellBody(in, cell, nameDeserializer.getNextCollectionElement(), b, flag, expireBefore);
    //            writer.writeTimestamp(cell.timestamp());
    //        }
    //        else
    //        {
    //            writer.writeTimestamp(Long.MIN_VALUE);
    //            ColumnDefinition column = getDefinition(nameDeserializer.getNextColumnName());
    //            if (columns.contains(column))
    //            {
    //                cell.column = column;
    //                metadata.layout().deserializeCellBody(in, cell, nameDeserializer.getNextCollectionElement(), b, flag, expireBefore);
    //                Cells.write(cell, writer);
    //            }
    //            else
    //            {
    //                metadata.layout().skipCellBody(in, b);
    //            }
    //        }

    //        // Read the rest of the cells belonging to this CQL row
    //        while (nameDeserializer.hasNext() && nameDeserializer.compareNextPrefixTo(prefix) == 0)
    //        {
    //            nameDeserializer.readNextClustering();
    //            b = in.readUnsignedByte();
    //            ColumnDefinition column = getDefinition(nameDeserializer.getNextColumnName());
    //            if ((b & LegacyLayout.RANGE_TOMBSTONE_MASK) != 0)
    //            {
    //                if (!columns.contains(column))
    //                {
    //                    metadata.layout().rangeTombstoneSerializer().skipBody(in, version);
    //                    continue;
    //                }

    //                // This is a collection tombstone
    //                RangeTombstone rt = metadata.layout().rangeTombstoneSerializer().deserializeBody(in, prefix, version);
    //                // TODO: we could assert that the min and max are what we think they are. Just in case
    //                // someone thrift side has done something *really* nasty.
    //                writer.writeComplexDeletion(column, rt.data);
    //            }
    //            else
    //            {
    //                if (!columns.contains(column))
    //                {
    //                    metadata.layout().skipCellBody(in, b);
    //                    continue;
    //                }

    //                cell.column = column;
    //                metadata.layout().deserializeCellBody(in, cell, nameDeserializer.getNextCollectionElement(), b, flag, expireBefore);
    //                Cells.write(cell, writer);
    //            }
    //        }
    //        return row;
    //    }

    //    private ColumnDefinition getDefinition(ByteBuffer columnName)
    //    {
    //        // For non-CQL3 layouts, every defined column metadata is handled by the static row
    //        if (!metadata.layout().isCQL3Layout())
    //            return metadata.compactValueColumn();

    //        return metadata.getColumnDefinition(columnName);
    //    }

    //    public void skipNext() throws IOException
    //    {
    //        Clustering prefix = nameDeserializer.readNextClustering();
    //        int b = in.readUnsignedByte();
    //        if ((b & LegacyLayout.RANGE_TOMBSTONE_MASK) != 0)
    //        {
    //            metadata.layout().rangeTombstoneSerializer().skipBody(in, version);
    //            return;
    //        }

    //        metadata.layout().skipCellBody(in, b);

    //        // Skip the rest of the cells belonging to this CQL row
    //        while (nameDeserializer.hasNext() && nameDeserializer.compareNextPrefixTo(prefix) == 0)
    //        {
    //            nameDeserializer.skipNext();
    //            b = in.readUnsignedByte();
    //            if ((b & LegacyLayout.RANGE_TOMBSTONE_MASK) != 0)
    //                metadata.layout().rangeTombstoneSerializer().skipBody(in, version);
    //            else
    //                metadata.layout().skipCellBody(in, b);
    //        }
    //    }
    //}

    //public interface Deserializer
    //{
    //    /**
    //     * Whether this deserializer is done or not, i.e. whether we're reached the end of row marker.
    //     */
    //    public boolean hasNext() throws IOException;
    //    /**
    //     * Whether or not some name has been read but not consumed by readNext.
    //     */
    //    public boolean hasUnprocessed() throws IOException;
    //    /**
    //     * Comparare the next name to read to the provided Composite.
    //     * This does not consume the next name.
    //     */
    //    public int compareNextTo(Clusterable composite) throws IOException;
    //    public int compareNextPrefixTo(Clustering prefix) throws IOException;
    //    /**
    //     * Actually consume the next name and return it.
    //     */
    //    public Clustering readNextClustering() throws IOException;
    //    public ByteBuffer getNextColumnName();
    //    public ByteBuffer getNextCollectionElement();

    //    /**
    //     * Skip the next name (consuming it). This skip all the name (clustering, column name and collection element).
    //     */
    //    public void skipNext() throws IOException;
    //}

    //public static class DeserializedCell extends AbstractCell
    //{
    //    public ColumnDefinition column;
    //    private boolean isCounter;
    //    private ByteBuffer value;
    //    private long timestamp;
    //    private int localDeletionTime;
    //    private int ttl;
    //    private CellPath path;

    //    public ColumnDefinition column()
    //    {
    //        return column;
    //    }

    //    public boolean isCounterCell()
    //    {
    //        return isCounter;
    //    }

    //    public ByteBuffer value()
    //    {
    //        return value;
    //    }

    //    public long timestamp()
    //    {
    //        return timestamp;
    //    }

    //    public int localDeletionTime()
    //    {
    //        return localDeletionTime;
    //    }

    //    public int ttl()
    //    {
    //        return ttl;
    //    }

    //    public CellPath path()
    //    {
    //        return path;
    //    }

    //    public Cell takeAlias()
    //    {
    //        // TODO
    //        throw new UnsupportedOperationException();
    //    }
    //}

    // From OnDiskAtom
    //public static class Serializer implements ISSTableSerializer<OnDiskAtom>
    //{
    //    private final CellNameType type;

    //    public Serializer(CellNameType type)
    //    {
    //        this.type = type;
    //    }

    //    public void serializeForSSTable(OnDiskAtom atom, DataOutputPlus out) throws IOException
    //    {
    //        if (atom instanceof Cell)
    //        {
    //            type.columnSerializer().serialize((Cell)atom, out);
    //        }
    //        else
    //        {
    //            assert atom instanceof RangeTombstone;
    //            type.rangeTombstoneSerializer().serializeForSSTable((RangeTombstone)atom, out);
    //        }
    //    }

    //    public OnDiskAtom deserializeFromSSTable(DataInput in, Descriptor.Version version) throws IOException
    //    {
    //        return deserializeFromSSTable(in, ColumnSerializer.Flag.LOCAL, Integer.MIN_VALUE, version);
    //    }

    //    public OnDiskAtom deserializeFromSSTable(DataInput in, ColumnSerializer.Flag flag, int expireBefore, Descriptor.Version version) throws IOException
    //    {
    //        Composite name = type.serializer().deserialize(in);
    //        if (name.isEmpty())
    //        {
    //            // SSTableWriter.END_OF_ROW
    //            return null;
    //        }

    //        int b = in.readUnsignedByte();
    //        if ((b & ColumnSerializer.RANGE_TOMBSTONE_MASK) != 0)
    //            return type.rangeTombstoneSerializer().deserializeBody(in, name, version);
    //        else
    //            return type.columnSerializer().deserializeColumnBody(in, (CellName)name, b, flag, expireBefore);
    //    }

    //    public long serializedSizeForSSTable(OnDiskAtom atom)
    //    {
    //        if (atom instanceof Cell)
    //        {
    //            return type.columnSerializer().serializedSize((Cell)atom, TypeSizes.NATIVE);
    //        }
    //        else
    //        {
    //            assert atom instanceof RangeTombstone;
    //            return type.rangeTombstoneSerializer().serializedSizeForSSTable((RangeTombstone)atom);
    //        }
    //    }
    //}

}
