package org.apache.cassandra.io.sstable.format.test;

import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.commons.collections.Unmodifiable;
import parquet.format.RowGroup;

import java.io.DataInput;
import java.io.IOException;
import java.util.List;

public class TestRowIndexEntrySerializer implements RowIndexEntry.IndexSerializer<TestRowIndexEntrySerializer.RowGroupList>
{
    @Override
    public void serialize(RowIndexEntry<RowGroupList> rie, DataOutputPlus out) throws IOException
    {

    }

    @Override
    public RowIndexEntry<RowGroupList> deserialize(DataInput in, Version version) throws IOException
    {
        return null;
    }

    @Override
    public int serializedSize(RowIndexEntry rie)
    {
        return 0;
    }

    public static class RowGroupList
    {
        public final List<RowGroup> groupList;

        public RowGroupList(List<RowGroup> groupList)
        {
            this.groupList = groupList;
        }
    }
}
