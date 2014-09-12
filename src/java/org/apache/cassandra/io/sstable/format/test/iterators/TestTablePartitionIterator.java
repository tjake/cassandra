package org.apache.cassandra.io.sstable.format.test.iterators;


import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.composites.CBuilder;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.test.DeletionTimeAwarePageReader;
import org.apache.cassandra.io.sstable.format.test.ParquetRowGroupReader;
import org.apache.cassandra.io.sstable.format.test.ParquetSequentialRowGroupReader;
import org.apache.cassandra.io.sstable.format.test.TestTableWriter;
import org.apache.cassandra.io.sstable.format.test.convert.Schema;
import org.apache.cassandra.io.sstable.format.test.convert.TestGroup;
import org.apache.cassandra.io.sstable.format.test.convert.TestGroupConverter;
import org.apache.cassandra.io.sstable.format.test.convert.TestRecordMaterializer;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import parquet.column.ColumnDescriptor;
import parquet.column.ColumnReader;
import parquet.column.page.Page;
import parquet.column.page.PageReadStore;
import parquet.example.data.Group;
import parquet.example.data.simple.convert.GroupRecordConverter;
import parquet.filter.ColumnPredicates;
import parquet.filter.RecordFilter;
import parquet.filter.UnboundRecordFilter;
import parquet.filter2.compat.FilterCompat;
import parquet.io.ColumnIOFactory;
import parquet.io.MessageColumnIO;
import parquet.io.ParquetDecodingException;
import parquet.io.RecordReader;
import parquet.io.api.Binary;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;
import parquet.schema.Type;


import static parquet.Preconditions.checkNotNull;
import static parquet.filter.AndRecordFilter.and;
import static parquet.filter.ColumnPredicates.applyFunctionToLong;
import static parquet.filter.ColumnPredicates.applyFunctionToString;
import static parquet.filter.ColumnPredicates.equalTo;
import static parquet.filter.NotRecordFilter.not;
import static parquet.filter.OrRecordFilter.or;
import static parquet.filter.PagedRecordFilter.page;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class TestTablePartitionIterator implements Iterator<OnDiskAtom>
{
    private ColumnSerializer.Flag flag;
    private int expireBefore;
    private final CFMetaData cfm;
    private final DeletionTimeAwarePageReader reader;
    private PageReadStore currentPageStore;

    private MessageColumnIO messageIO;
    private RecordMaterializer<TestGroup> recordConverter;
    private RecordReader<TestGroup> recordReader;
    private final FilterCompat.Filter filter;
    private final MessageType schema;
    private TestGroup next;
    private int numReadCurrentPage = 0;

    public TestTablePartitionIterator(DataInput in, ColumnSerializer.Flag flag, int expireBefore, CFMetaData cfm)
    {
        this(in, cfm, FilterCompat.NOOP);

        this.flag = flag;
        this.expireBefore = expireBefore;
    }

    public TestTablePartitionIterator(DataInput in, CFMetaData cfm, ColumnSlice[] slices)
    {
        //Convert
        this(in, cfm, convertSlices(slices, cfm));

        this.flag = ColumnSerializer.Flag.LOCAL;
        this.expireBefore = Integer.MIN_VALUE;
    }

    public TestTablePartitionIterator(DataInput in, CFMetaData cfm, Set<CellName> names)
    {
        //Convert
        this(in, cfm, convertNames(names, cfm));

        this.flag = ColumnSerializer.Flag.LOCAL;
        this.expireBefore = Integer.MIN_VALUE;
    }

    private TestTablePartitionIterator(DataInput in,  CFMetaData cfm, FilterCompat.Filter filter)
    {

        this.cfm = cfm;
        this.filter = filter;

        if (in instanceof FileDataInput)
        {
            reader = new ParquetRowGroupReader((FileDataInput) in);
        }
        else
        {
            assert filter.equals(FilterCompat.NOOP);
            reader = new ParquetSequentialRowGroupReader(in);
        }

        schema = Schema.getCachedParquetSchema(cfm);
        messageIO  = new ColumnIOFactory().getColumnIO(schema);
        recordConverter = new TestRecordMaterializer(schema);

        numReadCurrentPage = 0;
        next = null;
    }

    private static FilterCompat.Filter convertNames(Set<CellName> names, CFMetaData cfm)
    {
        UnboundRecordFilter namesFilter = null;

        for (CellName name : names)
        {
            UnboundRecordFilter nameFilter = createFilter(name, cfm);
            namesFilter = namesFilter == null ? nameFilter : or(namesFilter, nameFilter);
        }

        return namesFilter == null ? FilterCompat.NOOP : FilterCompat.get(namesFilter);
    }

    private static FilterCompat.Filter convertSlices(ColumnSlice[] slices, CFMetaData cfm)
    {

        UnboundRecordFilter slicesFilter = null;

        for (ColumnSlice slice : slices)
        {
            //Allow all == no filter
            if (slice.start.isEmpty() && slice.finish.isEmpty())
                return FilterCompat.NOOP;

            UnboundRecordFilter sliceFilter = createFilter(slice, cfm);

            slicesFilter = slicesFilter == null ? sliceFilter : or(slicesFilter, sliceFilter);
        }

        return slicesFilter == null ? FilterCompat.NOOP : FilterCompat.get(slicesFilter);
    }

    private static UnboundRecordFilter createFilter(final CellName name, final CFMetaData cfm)
    {

        assert !cfm.isCQL3Table();
        assert !name.isEmpty();

        ColumnPredicates.Predicate eqP = ColumnPredicates.applyFunctionToBinary(new ColumnPredicates.PredicateFunction<Binary>()
        {
            @Override
            public boolean functionToApply(Binary input)
            {
                return BytesType.instance.compare(input.toByteBuffer(), name.toByteBuffer()) == 0;
            }
        });


        ColumnPredicates.Predicate gtP = ColumnPredicates.applyFunctionToBinary(new ColumnPredicates.PredicateFunction<Binary>()
        {
            @Override
            public boolean functionToApply(Binary input)
            {
                return BytesType.instance.compare(input.toByteBuffer(), name.toByteBuffer()) >= 0;
            }
        });

        ColumnPredicates.Predicate ltP = ColumnPredicates.applyFunctionToBinary(new ColumnPredicates.PredicateFunction<Binary>()
        {
            @Override
            public boolean functionToApply(Binary input)
            {
                return BytesType.instance.compare(input.toByteBuffer(), name.toByteBuffer()) <= 0;
            }
        });


        ColumnPredicates.Predicate rtP = ColumnPredicates.applyFunctionToInteger(new ColumnPredicates.IntegerPredicateFunction()
        {
            @Override
            public boolean functionToApply(int input)
            {
                return input == Schema.CellTypes.RANGETOMBSTONE.ordinal();
            }
        });

        //Deal with range tombstones also
        return or(column(Schema.THRIFT_CELL_NAME, eqP),
                and(column(Schema.CELLTYPE_FIELD_NAME, rtP),
                        and(column(Schema.THRIFT_CELL_NAME, gtP), column(Schema.THRIFT_CELL_VALUE, ltP))));
    }

    /**
     * Factory method for record filter which applies the supplied predicate to the specified column.
     * Note that if searching for a repeated sub-attribute it will only ever match against the
     * first instance of it in the object.
     *
     * @param columnPath Dot separated path specifier, e.g. "engine.capacity"
     * @param predicate Should call getBinary etc. and check the value
     */
    public static final UnboundRecordFilter column(final String columnPath,
                                                   final ColumnPredicates.Predicate predicate) {
        checkNotNull(columnPath, "columnPath");
        checkNotNull(predicate,  "predicate");
        return new UnboundRecordFilter() {
            final String[] filterPath = columnPath.split("\\.");
            @Override
            public RecordFilter bind(Iterable<ColumnReader> readers) {
                for (ColumnReader reader : readers) {
                    if ( Arrays.equals( reader.getDescriptor().getPath(), filterPath)) {
                        return new ColumnRecordFilter(reader, predicate);
                    }
                }
                throw new IllegalArgumentException( "Column " + columnPath + " does not exist.");
            }
        };
    }

    //Require a custom class to fix https://github.com/Parquet/parquet-mr/issues/371
    private static class ColumnRecordFilter implements RecordFilter
    {
        private final ColumnReader filterOnColumn;
        private final ColumnPredicates.Predicate filterPredicate;

        public ColumnRecordFilter(ColumnReader filterOnColumn, ColumnPredicates.Predicate filterPredicate)
        {
            this.filterOnColumn = filterOnColumn;
            this.filterPredicate = filterPredicate;
        }

        @Override
        public boolean isMatch()
        {
            // first check whether the field is null in which case we consider the predicate false
            if (filterOnColumn.getCurrentDefinitionLevel() < filterOnColumn.getDescriptor().getMaxDefinitionLevel())
            {
                return false;
            }

            return filterPredicate.apply(filterOnColumn);
        }
    }

    private static UnboundRecordFilter createFilter(final ColumnSlice slice, final CFMetaData cfm)
    {
        assert !slice.start.isEmpty() || !slice.finish.isEmpty();

        UnboundRecordFilter filter = null;

        if (!cfm.isCQL3Table())
        {
            ColumnPredicates.Predicate startP = slice.start.isEmpty() ? null : ColumnPredicates.applyFunctionToBinary(new ColumnPredicates.PredicateFunction<Binary>()
            {
                @Override
                public boolean functionToApply(Binary input)
                {
                    return BytesType.instance.compare(input.toByteBuffer(), slice.start.toByteBuffer()) > 0;
                }
            });


            ColumnPredicates.Predicate finishP = slice.start.isEmpty() ? null : ColumnPredicates.applyFunctionToBinary(new ColumnPredicates.PredicateFunction<Binary>()
            {
                @Override
                public boolean functionToApply(Binary input)
                {
                    return BytesType.instance.compare(input.toByteBuffer(), slice.start.toByteBuffer()) <= 0;
                }
            });

            if (startP != null)
                filter = column(Schema.THRIFT_CELL_NAME, startP);

            if (finishP != null)
                filter = filter == null ? column(Schema.THRIFT_CELL_NAME, finishP) : and(filter, column(Schema.THRIFT_CELL_NAME, finishP));

            return filter;
        }
        else
        {

            for (int i = 0; i < cfm.clusteringColumns().size(); i++)
            {
                ColumnDefinition cdef = cfm.clusteringColumns().get(i);
                String name = cdef.name.toString();

                ByteBuffer start = slice.start.get(i);
                ByteBuffer finish = slice.finish.get(i);

                if (start != null && finish != null)
                {
                    ColumnPredicates.Predicate p = getPredicate(cdef.type.asCQL3Type(), slice.start.get(i), slice.finish.get(i));
                    filter = filter == null ? column(name, p) : and(filter, column(name, p));
                }
            }

            return filter;
        }
    }


    private static ColumnPredicates.Predicate getPredicate(final CQL3Type type, final ByteBuffer start, final ByteBuffer finish)
    {
        if (!(type instanceof CQL3Type.Native))
        {
            throw new UnsupportedOperationException();
        }

        return new ColumnPredicates.Predicate() {
            @Override
            public boolean apply(ColumnReader input) {
                switch ((CQL3Type.Native) type)
                {
                    default:
                        if (start.remaining() > 0 && finish.remaining() > 0)
                        {
                            ByteBuffer in = input.getBinary().toByteBuffer();
                            return BytesType.instance.compare(in, start) >= 0 && BytesType.instance.compare(in, finish) <= 0;
                        }
                };

                throw new UnsupportedOperationException();
            };
        };
    }

    @Override
    public boolean hasNext()
    {
        if (next != null)
            return true;

        //Read more from this row group if possible
        if (currentPageStore != null && numReadCurrentPage < currentPageStore.getRowCount())
        {
            try
            {
                next = recordReader.read();
            } catch (ParquetDecodingException e)
            {
                throw e;
            }

            if (next != null)
                return true;
        }

        //Try the next row group
        if (reader.hasNext())
        {
            currentPageStore = reader.next();
            recordReader = messageIO.getRecordReader(currentPageStore, recordConverter, filter);
            numReadCurrentPage = 0;
            return hasNext();
        }

        return false;
    }

    private ByteBuffer getValue(PrimitiveType type, TestGroup g)
    {

        //No type means this cell is a row marker. return empty value
        if (type == null)
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;

        switch (type.getPrimitiveTypeName())
        {
            case FIXED_LEN_BYTE_ARRAY:
            case BINARY:
                return g.getBinary(type.getName()).toByteBuffer();
            case INT32:
                return Int32Type.instance.decompose(g.getInteger(type.getName()));
            case INT64:
                return LongType.instance.decompose(g.getLong(type.getName()));
            case DOUBLE:
                return DoubleType.instance.decompose(g.getDouble(type.getName()));
            case FLOAT:
                return FloatType.instance.decompose(g.getFloat(type.getName()));
            case BOOLEAN:
                return BooleanType.instance.decompose(g.getBoolean(type.getName()));
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    public OnDiskAtom next()
    {
        if (!hasNext())
            return null;

        TestGroup record = next;
        next = null;

        if (cfm.hasStaticColumns())
            throw new UnsupportedOperationException();

        //Meta info
        long timestamp = record.getLong(record.next());
        Schema.CellTypes type = Schema.CellTypes.values()[record.getInteger(record.next())];

        OnDiskAtom atom = null;
        switch (type)
        {
            case NORMAL:
                atom = createNormalCell(record, timestamp, -1);
                break;
            case EXPIRING:
                int ttl = record.getInteger(record.next());
                atom = createNormalCell(record, timestamp, ttl);
                break;
            case TOMBSTONE:
                int localDeletionTime = record.getInteger(record.next());
                atom = createTombstone(record, timestamp, localDeletionTime);
                break;
            case RANGETOMBSTONE:
                localDeletionTime = record.getInteger(record.next());
                atom = createRangeTombstone(record, timestamp, localDeletionTime);
                break;
            default:
                throw new UnsupportedOperationException();
        }

        numReadCurrentPage++;

        return atom;
    }

    private OnDiskAtom createNormalCell(TestGroup record, long timestamp, int ttl)
    {
        if (cfm.isCQL3Table())
        {
            Pair<ByteBuffer[], ByteBuffer> name = getCellName(record);
            return AbstractCell.create(cfm.comparator.makeCellName(name.left), name.right, timestamp, ttl, cfm);
        }
        else
        {
            return AbstractCell.create(cfm.comparator.cellFromByteBuffer(record.getBinary(record.next()).toByteBuffer()), record.getBinary(record.next()).toByteBuffer(), timestamp, ttl, cfm);
        }
    }

    private OnDiskAtom createTombstone(TestGroup record, long timestamp, int localDeletionTime)
    {
        if (cfm.isCQL3Table())
        {
            Pair<ByteBuffer[], ByteBuffer> name = getCellName(record);
            return new BufferDeletedCell(cfm.comparator.makeCellName(name.left), localDeletionTime, timestamp);
        }
        else
        {
            return new BufferDeletedCell(cfm.comparator.cellFromByteBuffer(record.getBinary(record.next()).toByteBuffer()), localDeletionTime, timestamp);
        }
    }

    private OnDiskAtom createRangeTombstone(TestGroup record, long timestamp, int localDeletionTime)
    {
        return new RangeTombstone(cfm.comparator.make(record.getBinary(record.next()).toByteBuffer()),
                                  cfm.comparator.make(record.getBinary(record.next()).toByteBuffer()),
                                  new DeletionTime(timestamp, localDeletionTime));
    }

    private Pair<ByteBuffer[], ByteBuffer> getCellName(TestGroup record)
    {
        ByteBuffer name[] = new ByteBuffer[1 + cfm.clusteringColumns().size()];

        //Construct CellName from columnName
        for (int i = 0; i < cfm.clusteringColumns().size(); i++)
        {
            Type t = schema.getFields().get(record.next());
            assert t.isPrimitive();

            name[i] = getValue((PrimitiveType)t, record);
        }

        //Append the cell value
        //If this doesn't exist it's a row marker
        if (!record.hasNext())
        {
            name[name.length - 1] = ByteBufferUtil.EMPTY_BYTE_BUFFER;
            return Pair.create(name, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        }



        Type value = schema.getFields().get(record.next());
        assert value.isPrimitive();
        assert !record.hasNext();


        ByteBuffer valueFieldName = ByteBufferUtil.bytes(value.getName());


        //Collection elements go into the Cell name
        ColumnDefinition cdef = cfm.getColumnDefinition(valueFieldName);

        if (cdef.type.isCollection())
        {
            if (cdef.type instanceof MapType)
            {
                MapType mtype = ((MapType)cdef.type);

                Map map = mtype.getSerializer().deserializeForNativeProtocol(getValue((PrimitiveType) value, record), 3);
                //Serialized map is a single entry...
                Map.Entry entry = (Map.Entry) map.entrySet().iterator().next();

                return Pair.create(new ByteBuffer[] { valueFieldName, mtype.keys.decompose(entry.getKey())}, mtype.values.decompose(entry.getValue()));
            }

            //Other Collections pad the name
            return Pair.create(new ByteBuffer[] { valueFieldName, getValue((PrimitiveType) value, record) }, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        }

        //Cells with values
        name[name.length - 1] = valueFieldName;
        return Pair.create(name, getValue((PrimitiveType) value, record));
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    public DeletionTime getDeletionTime()
    {
        return reader.getDeletionTime();
    }
}
