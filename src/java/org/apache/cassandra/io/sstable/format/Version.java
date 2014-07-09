package org.apache.cassandra.io.sstable.format;

/**
 * A set of feature flags associated with a SSTable format
 *
 * versions are denoted as [major][minor].  Minor versions must be forward-compatible:
 * new fields are allowed in e.g. the metadata component, but fields can't be removed
 * or have their size changed.
 *
 * Minor versions were introduced with version "hb" for Cassandra 1.0.3; prior to that,
 * we always incremented the major version.
 *
 */
public abstract class Version
{
    protected final String version;

    protected Version(String version)
    {
        this.version = version;
    }

    public abstract boolean isLatestVersion();

    public abstract boolean hasPostCompressionAdlerChecksums();

    public abstract boolean hasSamplingLevel();

    public abstract boolean hasNewStatsFile();

    public abstract boolean hasAllAdlerChecksums();

    public abstract boolean hasRepairedAt();

    public abstract boolean tracksLegacyCounterShards();

    public abstract boolean hasNewFileName();

    public String getVersion()
    {
        return version;
    }


    /**
     * @param ver SSTable version
     * @return True if the given version string matches the format.
     * @see #version
     */
    public static boolean validate(String ver)
    {
        return ver != null && ver.matches("[a-z]+");
    }

    abstract public boolean isCompatible();

    @Override
    public String toString()
    {
        return version;
    }


    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Version version1 = (Version) o;

        if (version != null ? !version.equals(version1.version) : version1.version != null) return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        return version != null ? version.hashCode() : 0;
    }
}
