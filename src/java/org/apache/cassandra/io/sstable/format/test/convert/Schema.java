package org.apache.cassandra.io.sstable.format.test.convert;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQL3Type;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;
import parquet.schema.Type;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


public class Schema
{
    private static LoadingCache<CFMetaData, MessageType> cache = CacheBuilder.newBuilder()
                .expireAfterAccess(1, TimeUnit.MINUTES)
                .concurrencyLevel(DatabaseDescriptor.getConcurrentReaders())
                .initialCapacity(16 << 10)
                .build(new CacheLoader<CFMetaData, MessageType>()
                {
                    @Override
                    public MessageType load(CFMetaData key) throws Exception
                    {
                        return getParquetSchema(key);
                    }
                });

    //TODO move this to a proper class along with methods
    public static final String TIMESTAMP_FIELD_NAME = "_TSTAMP_";
    public static final String CELLTYPE_FIELD_NAME = "_CTYPE_";
    public static final String TTL_FIELD_NAME = "_TTL_";
    public static final String DELETION_TIME_FIELD_NAME = "_DTIME_";

    public static final String THRIFT_CELL_NAME = "_NAME_";
    public static final String THRIFT_CELL_VALUE = "_VALUE_";

    public static enum CellTypes {
        NORMAL,
        EXPIRING,
        TOMBSTONE,
        COUNTER,
        RANGETOMBSTONE
    };

    public static MessageType getCachedParquetSchema(CFMetaData metaData)
    {
        try
        {
            return cache.get(metaData);
        } catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static MessageType getParquetSchema(CFMetaData metadata)
    {
        List<Type> regular = new ArrayList<>();

        //Meta fields
        regular.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, Schema.TIMESTAMP_FIELD_NAME));
        regular.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, Schema.CELLTYPE_FIELD_NAME));

        //For expired Columns
        regular.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, Schema.TTL_FIELD_NAME));

        //For Tombstones
        regular.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, Schema.DELETION_TIME_FIELD_NAME));


        //Come up with the parquet schema
        //We can only map CQL3 types to Parquet, for Thrift we just throw it into a blob
        if (metadata.isCQL3Table())
        {

            //We need to get the list back in a deterministic order else parquet wont
            //match up the right fields
            Iterator<ColumnDefinition> allColumns = metadata.allColumnsInSelectOrder();

            while (allColumns.hasNext())
            {
                ColumnDefinition def = allColumns.next();

                switch (def.kind)
                {
                    case CLUSTERING_COLUMN:
                    case COMPACT_VALUE:
                    case REGULAR:
                        regular.add(new PrimitiveType(Type.Repetition.OPTIONAL, getPrimitiveType(def.type.asCQL3Type()), def.name.toString()));
                        break;
                    case PARTITION_KEY: //skip
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
            }

            // Used for RangeTombstones
            regular.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, Schema.THRIFT_CELL_NAME ));
            regular.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, Schema.THRIFT_CELL_VALUE ));

        }
        else  //Thrift
        {
            regular.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, Schema.THRIFT_CELL_NAME ));
            regular.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, Schema.THRIFT_CELL_VALUE ));
        }

        return new MessageType("schema", regular);
    }

    private static PrimitiveType.PrimitiveTypeName getPrimitiveType(CQL3Type type)
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
            case FLOAT:
                return PrimitiveType.PrimitiveTypeName.FLOAT;
            case DOUBLE:
                return PrimitiveType.PrimitiveTypeName.DOUBLE;
            //TODO hook in FIXED_LEN_BYTE_ARRAY for these
            case UUID:
            case TIMEUUID:
                return PrimitiveType.PrimitiveTypeName.BINARY;
            default:
                return PrimitiveType.PrimitiveTypeName.BINARY;
        }
    }

}
