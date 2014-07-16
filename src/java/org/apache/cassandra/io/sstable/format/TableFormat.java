package org.apache.cassandra.io.sstable.format;

import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.commons.lang.StringUtils;

/**
 * Created by jake on 7/3/14.
 */
public interface TableFormat
{
    Version getLatestVersion();
    Version getVersion(String version);

    Class<? extends SSTableWriter> getWriter();
    Class<? extends SSTableReader> getReader();

    static enum Type
    {
        //Used internally to refer to files with no
        //format flag in the filename
        LEGACY("big", BigFormat.instance),

        //The original sstable format
        BIG("big", BigFormat.instance),

        //The new sstable format
        TEST("test",  BigFormat.instance);

        public final TableFormat info;
        public final String name;
        private Type(String name, TableFormat info)
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
