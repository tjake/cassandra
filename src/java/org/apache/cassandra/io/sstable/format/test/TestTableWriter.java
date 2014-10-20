package org.apache.cassandra.io.sstable.format.test;

import com.google.common.collect.Lists;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.compaction.AbstractCompactedRow;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.LazilyCompactedRow;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.test.convert.Schema;
import org.apache.cassandra.io.sstable.format.test.convert.TestGroup;
import org.apache.cassandra.io.sstable.format.test.convert.TestGroupWriter;
import org.apache.cassandra.io.sstable.format.test.iterators.TestTablePartitionIterator;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.column.ParquetProperties;
import parquet.column.impl.ColumnWriteStoreImpl;

import parquet.io.ColumnIOFactory;
import parquet.io.MessageColumnIO;
import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;

import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;
import parquet.schema.Type;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;


public class TestTableWriter extends SSTableWriter
{
    private static final Logger logger = LoggerFactory.getLogger(TestTableWriter.class);

    private IndexWriter iwriter;
    private SegmentedFile.Builder dbuilder;
    private final SequentialWriter dataFile;
    private DecoratedKey lastWrittenKey;
    private FileMark dataMark;
    private final MessageType schema;

   public static final int PAGE_SIZE_IN_BYTES = (int) Math.pow(2, 17);  //128k

    TestTableWriter(Descriptor descriptor, Long keyCount, Long repairedAt, CFMetaData metadata, IPartitioner partitioner, MetadataCollector metadataCollector)
    {
        super(descriptor, keyCount, repairedAt, metadata, partitioner, metadataCollector);

        iwriter = new IndexWriter(keyCount);

        if (compression)
        {
            dataFile = SequentialWriter.open(getFilename(),
                    descriptor.filenameFor(Component.COMPRESSION_INFO),
                    metadata.compressionParameters(),
                    metadataCollector);
            dbuilder = SegmentedFile.getCompressedBuilder((CompressedSequentialWriter) dataFile);
        }
        else
        {
            dataFile = SequentialWriter.open(new File(getFilename()), new File(descriptor.filenameFor(Component.CRC)));
            dbuilder = SegmentedFile.getBuilder(DatabaseDescriptor.getDiskAccessMode());
        }


       schema = Schema.getCachedParquetSchema(metadata);

    }




    private static TestGroup add(OnDiskAtom atom, TestGroup g, CFMetaData metadata)
    {
        //Simply add bytes when this is a thrift table
        if (!metadata.isCQL3Table() || atom instanceof RangeTombstone)
        {
            if (atom instanceof Cell)
            {
                g.add(Schema.THRIFT_CELL_NAME, Binary.fromByteBuffer(atom.name().toByteBuffer()));
                g.add(Schema.THRIFT_CELL_VALUE, Binary.fromByteBuffer(((Cell) atom).value()));
            }
            else
            {
                assert atom instanceof RangeTombstone;

                RangeTombstone rt = (RangeTombstone) atom;
                g.add(Schema.THRIFT_CELL_NAME, Binary.fromByteBuffer(rt.min.toByteBuffer()));
                g.add(Schema.THRIFT_CELL_VALUE, Binary.fromByteBuffer(rt.max.toByteBuffer()));
            }

            return g;
        }


        //CQL
        assert atom instanceof Cell;

        Cell c = (Cell) atom;

        ByteBuffer value = c.value();

        //Handle clustering columns
        for (int i = 0; i < c.name().clusteringSize(); i++)
        {
            ColumnDefinition clusteringColDef = metadata.clusteringColumns().get(i);
            setParquetGroup(g, clusteringColDef.name.toString(), clusteringColDef.type.asCQL3Type(), c.name().get(i));
        }

        ColumnDefinition cdef = metadata.getColumnDefinition(c.name());
        if (cdef == null)
        {
            int columnIndex = metadata.comparator.clusteringPrefixSize();
            ByteBuffer CQL3ColumnName = c.name().get(columnIndex);

            cdef = metadata.getColumnDefinition(CQL3ColumnName);
        }

        if (cdef != null)
        {
            String name = cdef.name.toString();
            CQL3Type type = cdef.type.asCQL3Type();

            //Handle collection values (stored in cell name)
            if (c.name().isCollectionCell())
            {
                if (value.remaining() == 0)
                {
                    value = c.name().collectionElement();
                }
                else
                {

                    if (type.getType() instanceof MapType)
                    {
                        value = ((MapType) type.getType()).serializeForNativeProtocol(Collections.singletonList(c), 3);
                    }
                    else if (type.getType() instanceof ListType)
                    {
                        value = ((ListType) type.getType()).serializeForNativeProtocol(Collections.singletonList(c), 3);
                    }
                }
            }

            setParquetGroup(g, name, type, value);
        }


        return g;
    }

    public static void setParquetGroup(TestGroup g, String name, CQL3Type type, ByteBuffer value)
    {

        if (!(type instanceof CQL3Type.Native))
        {
            g.add(name, Binary.fromByteBuffer(value));
            return;
        }

        switch ((CQL3Type.Native) type)
        {
            case BLOB:
                g.add(name, Binary.fromByteBuffer(value));
                break;
            case INT:
                g.add(name, (Integer) type.getType().compose(value));
                break;
            case BIGINT:
                g.add(name, (Long)type.getType().compose(value));
                break;
            case TIMESTAMP:
                g.add(name, ((Date)type.getType().compose(value)).getTime());
                break;
            case BOOLEAN:
                g.add(name, (Boolean)type.getType().compose(value));
                break;
            case FLOAT:
                g.add(name, (Float)type.getType().compose(value));
                break;
            case DOUBLE:
                //Seems to be an issue in CLiTest sending a float to a double field.
                g.add(name, (value.remaining() == 4 ? FloatType.instance.compose(value) : DoubleType.instance.compose(value)));
                break;
            case UUID:
            case TIMEUUID:
                g.add(name, Binary.fromByteBuffer(value));
                break;
            default:
                g.add(name, Binary.fromByteBuffer(value));
        }
    }

    @Override
    public void mark()
    {
        dataMark = dataFile.mark();
        iwriter.mark();
    }

    @Override
    public RowIndexEntry append(AbstractCompactedRow row)
    {
        assert row instanceof TestCompactedRowWriter;
        ((TestCompactedRowWriter)row).setSchema(schema, metadata);

        long currentPosition = beforeAppend(row.key);
        RowIndexEntry entry;
        try
        {
            entry = row.write(currentPosition, dataFile);
            if (entry == null)
                return null;
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, dataFile.getPath());
        }
        metadataCollector.update(dataFile.getFilePointer() - currentPosition, row.columnStats());
        afterAppend(row.key, currentPosition, entry);
        return entry;
    }

    @Override
    public void append(DecoratedKey decoratedKey, ColumnFamily cf)
    {
        long startPosition = beforeAppend(decoratedKey);

        try
        {
            RowIndexEntry entry = rawAppend(cf, startPosition, decoratedKey, dataFile);

            afterAppend(decoratedKey, startPosition, entry);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, dataFile.getPath());
        }
        metadataCollector.update(dataFile.getFilePointer() - startPosition, cf.getColumnStats());
    }

    private RowIndexEntry rawAppend(ColumnFamily cf, long startPosition, DecoratedKey decoratedKey, SequentialWriter out) throws IOException
    {
        //Write the row key
        ByteBufferUtil.writeWithShortLength(decoratedKey.getKey(), out.stream);


        //Setup Parquet writer
        ParquetPageWriter store = new ParquetPageWriter();
        MessageColumnIO io  = new ColumnIOFactory().getColumnIO(schema);

        ColumnWriteStoreImpl writer = new ColumnWriteStoreImpl(store, PAGE_SIZE_IN_BYTES, PAGE_SIZE_IN_BYTES, 800, false, ParquetProperties.WriterVersion.PARQUET_1_0);
        RecordConsumer columnWriter = io.getRecordWriter(writer);

        TestGroupWriter gw = new TestGroupWriter(columnWriter, schema);

        // cf has disentangled the columns and range tombstones, we need to re-interleave them in comparator order
        Comparator<Composite> comparator = cf.getComparator();
        DeletionInfo.InOrderTester tester = cf.deletionInfo().inOrderTester();
        Iterator<RangeTombstone> rangeIter = cf.deletionInfo().rangeIterator();
        RangeTombstone tombstone = rangeIter.hasNext() ? rangeIter.next() : null;
        long rowsWritten = 0;

        for (Cell c : cf)
        {
            TestGroup g = new TestGroup(schema, c);

            while (tombstone != null && comparator.compare(c.name(), tombstone.min) >= 0)
            {
                // skip range tombstones that are shadowed by partition tombstones
                if (!cf.deletionInfo().getTopLevelDeletion().isDeleted(tombstone))
                {
                    gw.write(add(tombstone, new TestGroup(schema, tombstone), metadata));
                    rowsWritten++;
                }

                tombstone = rangeIter.hasNext() ? rangeIter.next() : null;
            }

            // We can skip any cell if it's shadowed by a tombstone already. This is a more
            // general case than was handled by CASSANDRA-2589.
            if (!tester.isDeleted(c))
            {
                add(c, g, metadata);

                gw.write(g);
                rowsWritten++;
            }
        }

        while (tombstone != null)
        {
            gw.write(add(tombstone, new TestGroup(schema, tombstone), metadata));
            rowsWritten++;
            tombstone = rangeIter.hasNext() ? rangeIter.next() : null;
        }

        //This writes the pages to memory
        writer.flush();

        //Store values in the outputStream
        store.write(out, rowsWritten);

        //We want to point to the start of the footer, ignoring the start magic value
        long footerOffset = out.getFilePointer() - startPosition + ParquetRowGroupReader.START_OF_FOOTER_MAGIC.length;

        //Output the footer at the end of the row.
        store.writeFooter(out, cf.deletionInfo().getTopLevelDeletion());

        return new TestRowIndexEntry(startPosition, cf.deletionInfo().getTopLevelDeletion(), new TestRowIndexEntry.ParquetInfo(footerOffset));
    }

    /**
     * Perform sanity checks on @param decoratedKey and @return the position in the data file before any data is written
     */
    private long beforeAppend(DecoratedKey decoratedKey)
    {
        assert decoratedKey != null : "Keys must not be null"; // empty keys ARE allowed b/c of indexed column values
        if (lastWrittenKey != null && lastWrittenKey.compareTo(decoratedKey) >= 0)
            throw new RuntimeException("Last written key " + lastWrittenKey + " >= current key " + decoratedKey + " writing into " + getFilename());
        return (lastWrittenKey == null) ? 0 : dataFile.getFilePointer();
    }

    private void afterAppend(DecoratedKey decoratedKey, long dataPosition, RowIndexEntry index)
    {
        metadataCollector.addKey(decoratedKey.getKey());
        lastWrittenKey = decoratedKey;
        last = lastWrittenKey;
        if (first == null)
            first = lastWrittenKey;

        if (logger.isTraceEnabled())
            logger.trace("wrote {} at {}", decoratedKey, dataPosition);
        iwriter.append(decoratedKey, index);
        dbuilder.addPotentialBoundary(dataPosition);
    }

    @Override
    public long appendFromStream(DecoratedKey key, CFMetaData metadata, DataInput fin, Version version) throws IOException
    {
        // deserialize each column to obtain maxTimestamp and immediately serialize it.
        long minTimestamp = Long.MAX_VALUE;
        long maxTimestamp = Long.MIN_VALUE;
        int maxLocalDeletionTime = Integer.MIN_VALUE;
        List<ByteBuffer> minColumnNames = Collections.emptyList();
        List<ByteBuffer> maxColumnNames = Collections.emptyList();
        StreamingHistogram tombstones = new StreamingHistogram(TOMBSTONE_HISTOGRAM_BIN_SIZE);
        boolean hasLegacyCounterShards = false;

        long startPosition = beforeAppend(key);

        //Write the row key
        ByteBufferUtil.writeWithShortLength(key.getKey(), dataFile.stream);

        //Setup Parquet writer
        ParquetPageWriter store = new ParquetPageWriter();
        MessageColumnIO io  = new ColumnIOFactory().getColumnIO(schema);

        ColumnWriteStoreImpl writer = new ColumnWriteStoreImpl(store, PAGE_SIZE_IN_BYTES, PAGE_SIZE_IN_BYTES, PAGE_SIZE_IN_BYTES, false, ParquetProperties.WriterVersion.PARQUET_1_0);
        RecordConsumer columnWriter = io.getRecordWriter(writer);

        TestGroupWriter gw = new TestGroupWriter(columnWriter, schema);

        //Iterator for the stream
        TestTablePartitionIterator reader = new TestTablePartitionIterator(fin, ColumnSerializer.Flag.PRESERVE_SIZE, Integer.MIN_VALUE, metadata);

        int rowsWritten = 0;

        while (reader.hasNext())
        {

            // deserialize column with PRESERVE_SIZE because we've written the cellDataSize based on the
            // data size received, so we must reserialize the exact same data
            OnDiskAtom atom = reader.next();
            if (atom == null)
                break;

            if (atom instanceof CounterCell)
            {
                atom = ((CounterCell) atom).markLocalToBeCleared();
                hasLegacyCounterShards = hasLegacyCounterShards || ((CounterCell) atom).hasLegacyShards();
            }

            int deletionTime = atom.getLocalDeletionTime();
            if (deletionTime < Integer.MAX_VALUE)
                tombstones.update(deletionTime);
            minTimestamp = Math.min(minTimestamp, atom.timestamp());
            maxTimestamp = Math.max(maxTimestamp, atom.timestamp());
            minColumnNames = ColumnNameHelper.minComponents(minColumnNames, atom.name(), metadata.comparator);
            maxColumnNames = ColumnNameHelper.maxComponents(maxColumnNames, atom.name(), metadata.comparator);
            maxLocalDeletionTime = Math.max(maxLocalDeletionTime, atom.getLocalDeletionTime());

            gw.write(add(atom, new TestGroup(schema, atom), metadata));
            rowsWritten++;
        }


        //This writes the pages to memory
        writer.flush();

        //Store values in the outputStream
        store.write(dataFile, rowsWritten);

        //We want to point to the start of the footer, ignoring the start magic value
        long footerOffset = dataFile.getFilePointer() - startPosition + ParquetRowGroupReader.START_OF_FOOTER_MAGIC.length;

        DeletionTime deletionInfo = reader.getDeletionTime();

        if (deletionInfo.localDeletionTime < Integer.MAX_VALUE)
            tombstones.update(deletionInfo.localDeletionTime);

        //Output the footer at the end of the row.
        store.writeFooter(dataFile, deletionInfo);

        afterAppend(key, startPosition, new TestRowIndexEntry(startPosition, deletionInfo, new TestRowIndexEntry.ParquetInfo(footerOffset)));

        metadataCollector.updateMinTimestamp(minTimestamp)
                .updateMaxTimestamp(maxTimestamp)
                .updateMaxLocalDeletionTime(maxLocalDeletionTime)
                .addRowSize(dataFile.getFilePointer() - startPosition)
                .addColumnCount(rowsWritten)
                .mergeTombstoneHistogram(tombstones)
                .updateMinColumnNames(minColumnNames)
                .updateMaxColumnNames(maxColumnNames)
                .updateHasLegacyCounterShards(hasLegacyCounterShards);

        return startPosition;
    }

    @Override
    public long getFilePointer()
    {
        return dataFile.getFilePointer();
    }

    @Override
    public long getOnDiskFilePointer()
    {
        return dataFile.getOnDiskFilePointer();
    }

    @Override
    public void isolateReferences()
    {
        first = getMinimalKey(first);
        last = lastWrittenKey = getMinimalKey(last);
    }

    @Override
    public void resetAndTruncate()
    {
        dataFile.resetAndTruncate(dataMark);
        iwriter.resetAndTruncate();
    }

    @Override
    public SSTableReader closeAndOpenReader(long maxDataAge, long repairedAt)
    {
        Pair<Descriptor, StatsMetadata> p = close(repairedAt);
        Descriptor newdesc = p.left;
        StatsMetadata sstableMetadata = p.right;

        // finalize in-memory state for the reader
        SegmentedFile ifile = iwriter.builder.complete(newdesc.filenameFor(Component.PRIMARY_INDEX));
        SegmentedFile dfile = dbuilder.complete(newdesc.filenameFor(Component.DATA));
        SSTableReader sstable = SSTableReader.internalOpen(newdesc,
                components,
                metadata,
                partitioner,
                ifile,
                dfile,
                iwriter.summary.build(partitioner),
                iwriter.bf,
                maxDataAge,
                sstableMetadata,
                SSTableReader.OpenReason.NORMAL);
        sstable.first = getMinimalKey(first);
        sstable.last = getMinimalKey(last);
        // try to save the summaries to disk
        sstable.saveSummary(iwriter.builder, dbuilder);
        iwriter = null;
        dbuilder = null;
        return sstable;
    }

    @Override
    public SSTableReader openEarly(long maxDataAge)
    {
        return null;
    }

    @Override
    public Pair<Descriptor, StatsMetadata> close()
    {
        return close(repairedAt);
    }

    private Pair<Descriptor, StatsMetadata> close(long repairedAt)
    {

        // index and filter
        iwriter.close();
        // main data, close will truncate if necessary
        dataFile.close();
        dataFile.writeFullChecksum(descriptor);
        // write sstable statistics
        Map<MetadataType, MetadataComponent> metadataComponents = metadataCollector.finalizeMetadata(
                partitioner.getClass().getCanonicalName(),
                metadata.getBloomFilterFpChance(),
                repairedAt);
        writeMetadata(descriptor, metadataComponents);

        // save the table of components
        SSTable.appendTOC(descriptor, components);

        // remove the 'tmp' marker from all components
        return Pair.create(SSTableWriter.rename(descriptor, components), (StatsMetadata) metadataComponents.get(MetadataType.STATS));

    }

    private static void writeMetadata(Descriptor desc, Map<MetadataType, MetadataComponent> components)
    {
        SequentialWriter out = SequentialWriter.open(new File(desc.filenameFor(Component.STATS)));
        try
        {
            desc.getMetadataSerializer().serialize(components, out.stream);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, out.getPath());
        }
        finally
        {
            out.close();
        }
    }

    /**
     * After failure, attempt to close the index writer and data file before deleting all temp components for the sstable
     */
    public void abort(boolean closeBf)
    {
        assert descriptor.type.isTemporary;
        if (iwriter == null && dataFile == null)
            return;
        if (iwriter != null)
        {
            FileUtils.closeQuietly(iwriter.indexFile);
            if (closeBf)
            {
                iwriter.bf.close();
            }
        }
        if (dataFile!= null)
            FileUtils.closeQuietly(dataFile);

        Set<Component> components = SSTable.componentsFor(descriptor);
        try
        {
            if (!components.isEmpty())
                SSTable.delete(descriptor, components);
        }
        catch (FSWriteError e)
        {
            logger.error(String.format("Failed deleting temp components for %s", descriptor), e);
            throw e;
        }
    }


    public static class TestCompactedRowWriter extends LazilyCompactedRow
    {
        MessageType schema;
        CFMetaData metadata;
        long rowsWritten = 0;


        public TestCompactedRowWriter(CompactionController controller, List<? extends OnDiskAtomIterator> rows)
        {
            super(controller, rows);
        }

        public void setSchema(MessageType schema, CFMetaData metadata)
        {
            this.schema = schema;
            this.metadata = metadata;
        }

        public RowIndexEntry write(long startPosition, SequentialWriter dataFile) throws IOException
        {
            assert !closed;
            assert schema != null;

            //Setup Parquet writer
            ParquetPageWriter store = new ParquetPageWriter();
            MessageColumnIO io  = new ColumnIOFactory().getColumnIO(schema);

            ColumnWriteStoreImpl writer = new ColumnWriteStoreImpl(store, PAGE_SIZE_IN_BYTES, PAGE_SIZE_IN_BYTES, PAGE_SIZE_IN_BYTES, false, ParquetProperties.WriterVersion.PARQUET_1_0);
            RecordConsumer columnWriter = io.getRecordWriter(writer);

            TestGroupWriter gw = new TestGroupWriter(columnWriter, schema);

            indexBuilder = new ColumnIndex.Builder(emptyColumnFamily, key.getKey(), dataFile.stream);

            while (merger.hasNext())
            {
                OnDiskAtom atom = merger.next();
                gw.write(add(atom, new TestGroup(schema, atom), metadata));
                rowsWritten++;

                indexBuilder.tombstoneTracker().update(atom, false);
            }

            //This writes the pages to memory
            writer.flush();

            //Store values in the outputStream
            store.write(dataFile, rowsWritten);

            //We want to point to the start of the footer, ignoring the start magic value
            long footerOffset = dataFile.getFilePointer() - startPosition + ParquetRowGroupReader.START_OF_FOOTER_MAGIC.length;

            DeletionTime deletionInfo = emptyColumnFamily.deletionInfo().getTopLevelDeletion();

            //Output the footer at the end of the row.
            store.writeFooter(dataFile, deletionInfo);

            // reach into the reducer (created during iteration) to get column count, size, max column timestamp
            columnStats = new ColumnStats(reducer.columns,
                    reducer.minTimestampTracker.get(),
                    Math.max(emptyColumnFamily.deletionInfo().maxTimestamp(), reducer.maxTimestampTracker.get()),
                    reducer.maxDeletionTimeTracker.get(),
                    reducer.tombstones,
                    reducer.minColumnNameSeen,
                    reducer.maxColumnNameSeen,
                    reducer.hasLegacyCounterShards);


            close();

            return new TestRowIndexEntry(startPosition, deletionInfo, new TestRowIndexEntry.ParquetInfo(footerOffset));
        }
    }

    /*
    * Encapsulates writing the index and filter for an SSTable. The state of this object is not valid until it has been closed.
    */
    class IndexWriter implements Closeable
    {
        private final SequentialWriter indexFile;
        public final SegmentedFile.Builder builder;
        public final IndexSummaryBuilder summary;
        public final IFilter bf;
        private FileMark mark;

        IndexWriter(long keyCount)
        {
            indexFile = SequentialWriter.open(new File(descriptor.filenameFor(Component.PRIMARY_INDEX)));
            builder = SegmentedFile.getBuilder(DatabaseDescriptor.getIndexAccessMode());
            summary = new IndexSummaryBuilder(keyCount, metadata.getMinIndexInterval(), Downsampling.BASE_SAMPLING_LEVEL);
            bf = FilterFactory.getFilter(keyCount, metadata.getBloomFilterFpChance(), true);
        }

        // finds the last (-offset) decorated key that can be guaranteed to occur fully in the flushed portion of the index file
        DecoratedKey getMaxReadableKey(int offset)
        {
            long maxIndexLength = indexFile.getLastFlushOffset();
            return summary.getMaxReadableKey(maxIndexLength, offset);
        }

        public void append(DecoratedKey key, RowIndexEntry indexEntry)
        {
            bf.add(key.getKey());
            long indexPosition = indexFile.getFilePointer();
            try
            {
                ByteBufferUtil.writeWithShortLength(key.getKey(), indexFile.stream);
                rowIndexEntrySerializer.serialize(indexEntry, indexFile.stream);
            }
            catch (IOException e)
            {
                throw new FSWriteError(e, indexFile.getPath());
            }

            if (logger.isTraceEnabled())
                logger.trace("wrote index entry: {} at {}", indexEntry, indexPosition);

            summary.maybeAddEntry(key, indexPosition);
            builder.addPotentialBoundary(indexPosition);
        }

        /**
         * Closes the index and bloomfilter, making the public state of this writer valid for consumption.
         */
        public void close()
        {
            if (components.contains(Component.FILTER))
            {
                String path = descriptor.filenameFor(Component.FILTER);
                try
                {
                    // bloom filter
                    FileOutputStream fos = new FileOutputStream(path);
                    DataOutputStreamAndChannel stream = new DataOutputStreamAndChannel(fos);
                    FilterFactory.serialize(bf, stream);
                    stream.flush();
                    fos.getFD().sync();
                    stream.close();
                }
                catch (IOException e)
                {
                    throw new FSWriteError(e, path);
                }
            }

            // index
            long position = indexFile.getFilePointer();
            indexFile.close(); // calls force
            FileUtils.truncate(indexFile.getPath(), position);
        }

        public void mark()
        {
            mark = indexFile.mark();
        }

        public void resetAndTruncate()
        {
            // we can't un-set the bloom filter addition, but extra keys in there are harmless.
            // we can't reset dbuilder either, but that is the last thing called in afterappend so
            // we assume that if that worked then we won't be trying to reset.
            indexFile.resetAndTruncate(mark);
        }
    }
}
