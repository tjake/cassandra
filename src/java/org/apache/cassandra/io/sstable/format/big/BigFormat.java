package org.apache.cassandra.io.sstable.format.big;

import org.apache.cassandra.io.sstable.format.TableFormat;
import org.apache.cassandra.io.sstable.format.TableReader;
import org.apache.cassandra.io.sstable.format.TableWriter;
import org.apache.cassandra.io.sstable.format.Version;

/**
 * Legacy bigtable format C* used till
 */
public class BigFormat implements TableFormat
{
    public static BigFormat instance = new BigFormat();
    public static BigVersion latestVersion = new BigVersion(BigVersion.current_version);

    private BigFormat()
    {

    }

    @Override
    public Version getLatestVersion()
    {
        return latestVersion;
    }

    @Override
    public Version getVersion(String version)
    {
        return new BigVersion(version);
    }

    @Override
    public Class<? extends TableWriter> getWriter()
    {
        return BigTableWriter.class;
    }

    @Override
    public Class<? extends TableReader> getReader()
    {
        return BigTableReader.class;
    }

    // versions are denoted as [major][minor].  Minor versions must be forward-compatible:
    // new fields are allowed in e.g. the metadata component, but fields can't be removed
    // or have their size changed.
    //
    // Minor versions were introduced with version "hb" for Cassandra 1.0.3; prior to that,
    // we always incremented the major version.
    static class BigVersion extends Version
    {
        public static final String current_version = "la";
        public static final String earliest_supported_version = "ja";

        // ja (2.0.0): super columns are serialized as composites (note that there is no real format change,
        //               this is mostly a marker to know if we should expect super columns or not. We do need
        //               a major version bump however, because we should not allow streaming of super columns
        //               into this new format)
        //             tracks max local deletiontime in sstable metadata
        //             records bloom_filter_fp_chance in metadata component
        //             remove data size and column count from data file (CASSANDRA-4180)
        //             tracks max/min column values (according to comparator)
        // jb (2.0.1): switch from crc32 to adler32 for compression checksums
        //             checksum the compressed data
        // ka (2.1.0): new Statistics.db file format
        //             index summaries can be downsampled and the sampling level is persisted
        //             switch uncompressed checksums to adler32
        //             tracks presense of legacy (local and remote) counter shards
        // la (3.0.0): new file name format

        private final boolean isLatestVersion;
        private final boolean hasPostCompressionAdlerChecksums;
        private final boolean hasSamplingLevel;
        private final boolean newStatsFile;
        private final boolean hasAllAdlerChecksums;
        private final boolean hasRepairedAt;
        private final boolean tracksLegacyCounterShards;
        private final boolean newFileName;

        public BigVersion(String version)
        {
            super(version);

            isLatestVersion = version.compareTo(current_version) == 0;
            hasPostCompressionAdlerChecksums = version.compareTo("jb") >= 0;
            hasSamplingLevel = version.compareTo("ka") >= 0;
            newStatsFile = version.compareTo("ka") >= 0;
            hasAllAdlerChecksums = version.compareTo("ka") >= 0;
            hasRepairedAt = version.compareTo("ka") >= 0;
            tracksLegacyCounterShards = version.compareTo("ka") >= 0;
            newFileName = version.compareTo("la") >= 0;
        }

        @Override
        public boolean isLatestVersion()
        {
            return isLatestVersion;
        }

        @Override
        public boolean hasPostCompressionAdlerChecksums()
        {
            return hasPostCompressionAdlerChecksums;
        }

        @Override
        public boolean hasSamplingLevel()
        {
            return hasSamplingLevel;
        }

        @Override
        public boolean hasNewStatsFile()
        {
            return newStatsFile;
        }

        @Override
        public boolean hasAllAdlerChecksums()
        {
            return hasAllAdlerChecksums;
        }

        @Override
        public boolean hasRepairedAt()
        {
            return hasRepairedAt;
        }

        @Override
        public boolean tracksLegacyCounterShards()
        {
            return tracksLegacyCounterShards;
        }

        @Override
        public boolean hasNewFileName()
        {
            return newFileName;
        }

        @Override
        public boolean isCompatible()
        {
            return version.compareTo(earliest_supported_version) >= 0 && version.charAt(0) <= current_version.charAt(0);
        }
    }
}
