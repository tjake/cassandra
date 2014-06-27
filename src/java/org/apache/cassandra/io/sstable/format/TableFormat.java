package org.apache.cassandra.io.sstable.format;

import org.apache.cassandra.io.sstable.Descriptor;

/**
 * Created by jake on 7/3/14.
 */
public interface TableFormat
{
    Descriptor.Version getVersion(String version);

    Class<TableWriter> getWriter();
    Class<TableReader> getReader();
}
