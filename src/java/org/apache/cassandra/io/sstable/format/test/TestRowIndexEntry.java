package org.apache.cassandra.io.sstable.format.test;

import com.google.common.primitives.Ints;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ObjectSizes;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class TestRowIndexEntry extends RowIndexEntry<TestRowIndexEntry.ParquetInfo>
{
    private static final long BASE_SIZE =
            ObjectSizes.measure(new TestRowIndexEntry(0, DeletionTime.LIVE, new ParquetInfo(0)))
                    + ObjectSizes.measure(new ArrayList<>(1));

    private final DeletionTime deletionTime;
    private final List<ParquetInfo> parquetInfo;

    public TestRowIndexEntry(long position, DeletionTime deletionTime, ParquetInfo parquetInfo)
    {
        super(position);

        this.deletionTime = deletionTime;
        this.parquetInfo = Collections.singletonList(parquetInfo);
    }

    @Override
    public long headerOffset()
    {
        return parquetInfo.get(0).headerOffset;
    }

    @Override
    public List<ParquetInfo> columnsIndex()
    {
        return parquetInfo;
    }

    @Override
    public DeletionTime deletionTime()
    {
        return deletionTime;
    }


    @Override
    public int promotedSize(ISerializer<ParquetInfo> idxSerializer)
    {
        TypeSizes typeSizes = TypeSizes.NATIVE;
        long size = DeletionTime.serializer.serializedSize(deletionTime, typeSizes);
        size += typeSizes.sizeof(parquetInfo.size()); // number of entries

        for (ParquetInfo info : parquetInfo)
            size += idxSerializer.serializedSize(info, typeSizes);

        return Ints.checkedCast(size);
    }

    @Override
    public long unsharedHeapSize()
    {
        long entrySize = 0;
        for (ParquetInfo idx : parquetInfo)
            entrySize += idx.unsharedHeapSize();

        return BASE_SIZE
                + entrySize
                + deletionTime.unsharedHeapSize()
                + ObjectSizes.sizeOfReferenceArray(parquetInfo.size());
    }



    public static class ParquetInfo
    {
        public final long headerOffset;
        private static final long EMPTY_SIZE = ObjectSizes.measure(new ParquetInfo(0));

        public ParquetInfo(long headerOffset)
        {
            this.headerOffset = headerOffset;
        }

        public long unsharedHeapSize()
        {
            return EMPTY_SIZE;
        }

        static class Serializer implements ISerializer<ParquetInfo>
        {

            @Override
            public void serialize(ParquetInfo parquetInfo, DataOutputPlus out) throws IOException
            {
                out.writeLong(parquetInfo.headerOffset);
            }

            @Override
            public ParquetInfo deserialize(DataInput in) throws IOException
            {
                return new ParquetInfo(in.readLong());
            }

            @Override
            public long serializedSize(ParquetInfo parquetInfo, TypeSizes typeSizes)
            {
                return typeSizes.sizeof(parquetInfo.headerOffset);
            }
        }
    }

    public static class TestRowIndexEntrySerializer implements RowIndexEntry.IndexSerializer<ParquetInfo>
    {
        private final ISerializer<ParquetInfo> idxSerializer = new ParquetInfo.Serializer();


        @Override
        public void serialize(RowIndexEntry<ParquetInfo> rie, DataOutputPlus out) throws IOException
        {
            out.writeLong(rie.position);
            out.writeInt(rie.promotedSize(idxSerializer));

            if (rie.isIndexed())
            {
                DeletionTime.serializer.serialize(rie.deletionTime(), out);
                out.writeInt(rie.columnsIndex().size());
                for (ParquetInfo info : rie.columnsIndex())
                    idxSerializer.serialize(info, out);
            }
        }

        @Override
        public RowIndexEntry<ParquetInfo> deserialize(DataInput in, Version version) throws IOException
        {
            long position = in.readLong();

            int size = in.readInt();
            if (size > 0)
            {
                DeletionTime deletionTime = DeletionTime.serializer.deserialize(in);

                int entries = in.readInt();
                assert entries == 1;

                return new TestRowIndexEntry(position, deletionTime, idxSerializer.deserialize(in));
            }
            else
            {
                return new RowIndexEntry<>(position);
            }
        }

        @Override
        public int  serializedSize(RowIndexEntry<ParquetInfo> rie)
        {
            return TypeSizes.NATIVE.sizeof(rie.position) + rie.promotedSize(idxSerializer);
        }
    }
}