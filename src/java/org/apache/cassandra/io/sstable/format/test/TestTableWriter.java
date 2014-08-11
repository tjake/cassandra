package org.apache.cassandra.io.sstable.format.test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.AbstractCompactedRow;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.Version;
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
import parquet.column.page.PageReadStore;
import parquet.example.data.Group;
import parquet.example.data.GroupWriter;
import parquet.example.data.simple.SimpleGroup;
import parquet.example.data.simple.convert.GroupRecordConverter;
import parquet.io.ColumnIOFactory;
import parquet.io.MessageColumnIO;
import parquet.io.RecordReader;
import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;
import parquet.io.api.RecordMaterializer;
import parquet.schema.GroupType;
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


    TestTableWriter(Descriptor descriptor, Long keyCount, Long repairedAt, CFMetaData metadata, IPartitioner partitioner, MetadataCollector metadataCollector)
    {
        super(descriptor, keyCount, repairedAt, metadata, partitioner, metadataCollector);

        if (metadata.regularColumns().isEmpty())
            throw new UnsupportedOperationException("Only Cql3 tables supported");

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


        GroupType compositeGroup = new GroupType(Type.Repetition.REPEATED, "composite");

        List<Type> composite = new ArrayList<>();
        List<Type> regular = new ArrayList<>();

        //Come up with the parquet schema
        for (ColumnDefinition def : metadata.allColumns())
        {
            switch (def.kind)
            {
                case REGULAR:
                    regular.add(new PrimitiveType(Type.Repetition.OPTIONAL, getPrimitiveType(def.type.asCQL3Type()), def.name.toString()));

                case CLUSTERING_COLUMN:
                case COMPACT_VALUE:
                case PARTITION_KEY: //skip
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }

        schema = new MessageType("schema", regular);

    }

    private PrimitiveType.PrimitiveTypeName getPrimitiveType(CQL3Type type)
    {
        if (!(type instanceof CQL3Type.Native))
            return PrimitiveType.PrimitiveTypeName.BINARY;

        switch ((CQL3Type.Native) type)
        {
            case BLOB:
                return PrimitiveType.PrimitiveTypeName.BINARY;
            case INT:
                return PrimitiveType.PrimitiveTypeName.INT32;
            case BIGINT:
            case TIMESTAMP:
                return PrimitiveType.PrimitiveTypeName.INT64;
            case BOOLEAN:
                return PrimitiveType.PrimitiveTypeName.BOOLEAN;
            case DOUBLE:
                return PrimitiveType.PrimitiveTypeName.DOUBLE;
            case UUID:
            case TIMEUUID:
                return PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
            default:
                return PrimitiveType.PrimitiveTypeName.BINARY;
        }
    }

    private void addPrimitiveValue(Cell c, Group g)
    {

        ColumnDefinition cdef = metadata.getColumnDefinition(c.name());

        assert cdef != null;

        String name = cdef.name.toString();
        CQL3Type type = cdef.type.asCQL3Type();

        if (!(type instanceof CQL3Type.Native))
        {
            g.add(name, Binary.fromByteBuffer(c.value()));
            return;
        }

        switch ((CQL3Type.Native) type)
        {
            case BLOB:
                g.add(name, Binary.fromByteBuffer(c.value()));
                break;
            case INT:
                g.add(name, (Integer) type.getType().compose(c.value()));
                break;
            case BIGINT:
            case TIMESTAMP:
                g.add(name, (Long)type.getType().compose(c.value()));
                break;
            case BOOLEAN:
                g.add(name, (Boolean)type.getType().compose(c.value()));
                break;
            case DOUBLE:
                g.add(name, (Double)type.getType().compose(c.value()));
                break;
            case UUID:
            case TIMEUUID:
                g.add(name, Binary.fromByteBuffer(c.value()));
                break;
            default:
                g.add(name, Binary.fromByteBuffer(c.value()));
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
        throw new UnsupportedOperationException();
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
        out.write(decoratedKey.getKey());

        //Write the row level deletion info
        DeletionTime.serializer.serialize(cf.deletionInfo().getTopLevelDeletion(), out.stream);

        //Setup Parquet writer
        ParquetPageWriter store = new ParquetPageWriter();
        MessageColumnIO io  = new ColumnIOFactory().getColumnIO(schema);

        ColumnWriteStoreImpl writer = new ColumnWriteStoreImpl(store, 800, 800, 800, false, ParquetProperties.WriterVersion.PARQUET_1_0);
        RecordConsumer columnWriter = io.getRecordWriter(writer);

        GroupWriter gw = new GroupWriter(columnWriter, schema);

        // cf has disentangled the columns and range tombstones, we need to re-interleave them in comparator order
        Comparator<Composite> comparator = cf.getComparator();
        DeletionInfo.InOrderTester tester = cf.deletionInfo().inOrderTester();
        Iterator<RangeTombstone> rangeIter = cf.deletionInfo().rangeIterator();
        RangeTombstone tombstone = rangeIter.hasNext() ? rangeIter.next() : null;
        Group g = new SimpleGroup(schema);
        long rowsWritten = 0;

        //TODO Manage clustered cells
        for (Cell c : cf)
        {
            while (tombstone != null && comparator.compare(c.name(), tombstone.min) >= 0)
            {
                // skip range tombstones that are shadowed by partition tombstones
                if (!cf.deletionInfo().getTopLevelDeletion().isDeleted(tombstone))
                {
                    gw.write(add(tombstone, new SimpleGroup(schema)));
                    rowsWritten++;
                }

                tombstone = rangeIter.hasNext() ? rangeIter.next() : null;
            }

            // We can skip any cell if it's shadowed by a tombstone already. This is a more
            // general case than was handled by CASSANDRA-2589.
            if (!tester.isDeleted(c) && c.value().remaining() > 0)
                add(c, g);
        }

        gw.write(g);
        rowsWritten++;

        while (tombstone != null)
        {
            gw.write(add(tombstone, new SimpleGroup(schema)));
            rowsWritten++;
            tombstone = rangeIter.hasNext() ? rangeIter.next() : null;
        }

        //This writes the pages to memory
        writer.flush();

        //Store values in the outputStream
        store.write(out, rowsWritten);

        //Output the footer at the end of the row.
        store.writeFooter(out);

        return new RowIndexEntry(startPosition);
    }

    private Group add(OnDiskAtom c, Group g) throws IOException
    {

        if (c instanceof RangeTombstone)
        {
            throw new UnsupportedOperationException();
        }
        else
        {
            assert c instanceof Cell;
            addPrimitiveValue((Cell) c, g);
        }

        return g;
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
    public long appendFromStream(DecoratedKey key, CFMetaData metadata, FileDataInput fin, Version version) throws IOException
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
        DeletionTime deletionInfo = DeletionTime.serializer.deserialize(fin);

        if (deletionInfo.localDeletionTime < Integer.MAX_VALUE)
            tombstones.update(deletionInfo.localDeletionTime);

        //Write the row key
        dataFile.write(key.getKey());

        //Write the row level deletion info
        DeletionTime.serializer.serialize(deletionInfo, dataFile.stream);

        //Setup Parquet writer
        ParquetPageWriter store = new ParquetPageWriter();
        MessageColumnIO io  = new ColumnIOFactory().getColumnIO(schema);

        ColumnWriteStoreImpl writer = new ColumnWriteStoreImpl(store, 800, 800, 800, false, ParquetProperties.WriterVersion.PARQUET_1_0);
        RecordConsumer columnWriter = io.getRecordWriter(writer);

        GroupWriter gw = new GroupWriter(columnWriter, schema);


        ParquetRowGroupReader rowGroupReader  = new ParquetRowGroupReader(version, fin);

        MessageColumnIO w  = new ColumnIOFactory().getColumnIO(schema);

        RecordMaterializer<Group> recordConverter = new GroupRecordConverter(schema);
        int rowsWritten = 0;

        for (PageReadStore readStore : rowGroupReader)
        {
            RecordReader<Group> reader = w.getRecordReader(readStore, recordConverter);

            for (int i = 0; i < readStore.getRowCount(); i++)
            {
                Group g = reader.read();
                gw.write(g);
                rowsWritten++;
            }
        }


        //This writes the pages to memory
        writer.flush();

        //Store values in the outputStream
        store.write(dataFile, rowsWritten);

        //Output the footer at the end of the row.
        store.writeFooter(dataFile);

        afterAppend(key, startPosition, new RowIndexEntry(startPosition));


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
                false);
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
        throw new UnsupportedOperationException();

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
                metadata.comparator.rowIndexEntrySerializer().serialize(indexEntry, indexFile.stream);
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
