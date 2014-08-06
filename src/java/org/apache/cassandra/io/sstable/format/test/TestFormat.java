package org.apache.cassandra.io.sstable.format.test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;

import java.util.Set;

/**
 * Created by jake on 7/18/14.
 */
public class TestFormat implements SSTableFormat
{

    public static final TestFormat instance = new TestFormat();
    private static final SSTableReader.Factory readerFactory = new ReaderFactory();
    private static final SSTableWriter.Factory writerFactory = new WriterFactory();


    private TestFormat()
    {

    }

    @Override
    public Version getLatestVersion()
    {
        return new TestVersion("aa");
    }

    @Override
    public Version getVersion(String version)
    {
        return new TestVersion(version);
    }

    @Override
    public SSTableWriter.Factory getWriterFactory()
    {
        return writerFactory;
    }

    @Override
    public SSTableReader.Factory getReaderFactory()
    {
        return readerFactory;
    }

    private static class WriterFactory extends SSTableWriter.Factory
    {

        @Override
        public SSTableWriter open(Descriptor descriptor, long keyCount, long repairedAt, CFMetaData metadata, IPartitioner partitioner, MetadataCollector metadataCollector)
        {
            return new TestTableWriter(descriptor, keyCount, repairedAt, metadata, partitioner, metadataCollector);
        }
    }


    private static class ReaderFactory extends SSTableReader.Factory
    {
        @Override
        public SSTableReader open(Descriptor descriptor, Set<Component> components, CFMetaData metadata, IPartitioner partitioner, Long maxDataAge, StatsMetadata sstableMetadata, Boolean isOpenEarly)
        {
            return new TestTableReader(descriptor, components, metadata, partitioner, maxDataAge, sstableMetadata, isOpenEarly);
        }
    }

    public static class TestVersion extends Version
    {

        protected TestVersion(String version)
        {
            super(version);
        }

        @Override
        public boolean isLatestVersion()
        {
            return true;
        }

        @Override
        public boolean hasPostCompressionAdlerChecksums()
        {
            return true;
        }

        @Override
        public boolean hasSamplingLevel()
        {
            return true;
        }

        @Override
        public boolean hasNewStatsFile()
        {
            return true;
        }

        @Override
        public boolean hasAllAdlerChecksums()
        {
            return true;
        }

        @Override
        public boolean hasRepairedAt()
        {
            return true;
        }

        @Override
        public boolean tracksLegacyCounterShards()
        {
            return false;
        }

        @Override
        public boolean hasNewFileName()
        {
            return true;
        }

        @Override
        public boolean isSequential()
        {
            return false;
        }

        @Override
        public boolean isCompatible()
        {
            return true;
        }
    }
}
