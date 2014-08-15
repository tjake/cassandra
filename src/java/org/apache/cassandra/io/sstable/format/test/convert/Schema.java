package org.apache.cassandra.io.sstable.format.test.convert;

/**
 * Created by jake on 8/14/14.
 */
public class Schema
{
    //TODO move this to a proper class along with methods
    public static final String TIMESTAMP_FIELD_NAME = "_TSTAMP_";
    public static final String CELLTYPE_FIELD_NAME = "_CTYPE_";
    public static final String TTL_FIELD_NAME = "_TTL_";

    public static enum CellTypes {
        NORMAL,
        EXPIRING,
        TOMBSTONE,
        COUNTER,
        RANGETOMBSTONE
    };


}
