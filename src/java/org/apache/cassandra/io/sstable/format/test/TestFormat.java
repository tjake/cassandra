package org.apache.cassandra.io.sstable.format.test;

import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.Version;

/**
 * Created by jake on 7/18/14.
 */
public class TestFormat implements SSTableFormat
{

    public static final TestFormat instance = new TestFormat();

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
    public Class<? extends SSTableWriter> getWriter()
    {
        return TestTableWriter.class;
    }

    @Override
    public Class<? extends SSTableReader> getReader()
    {
        return TestTableReader.class;
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
