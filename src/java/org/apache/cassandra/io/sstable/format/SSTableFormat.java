package org.apache.cassandra.io.sstable.format;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnSerializer;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.composites.CType;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.test.TestFormat;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.util.Iterator;

/**
 * Created by jake on 7/3/14.
 */
public interface SSTableFormat
{
    Version getLatestVersion();
    Version getVersion(String version);

    SSTableWriter.Factory getWriterFactory();
    SSTableReader.Factory getReaderFactory();

    Iterator<OnDiskAtom> getOnDiskIterator(DataInput in, ColumnSerializer.Flag flag, int expireBefore, CFMetaData cfm, Version version);

    RowIndexEntry.IndexSerializer  getIndexSerializer(CFMetaData cfm);

    static enum Type
    {
        //Used internally to refer to files with no
        //format flag in the filename
        LEGACY("big", BigFormat.instance),

        //The original sstable format
        BIG("big", BigFormat.instance),

        //The new sstable format
        TEST("test",  TestFormat.instance);

        public final SSTableFormat info;
        public final String name;
        private Type(String name, SSTableFormat info)
        {
            //Since format comes right after generation
            //we disallow formats with numeric names
            assert !StringUtils.isNumeric(name);

            this.name = name;
            this.info = info;
        }

        public static Type validate(String name)
        {
            for (Type valid : Type.values())
            {
                //This is used internally for old sstables
                if (valid == LEGACY)
                    continue;

                if (valid.name.equalsIgnoreCase(name))
                    return valid;
            }

            throw new IllegalArgumentException("No Type constant " + name);
        }
    }
}
