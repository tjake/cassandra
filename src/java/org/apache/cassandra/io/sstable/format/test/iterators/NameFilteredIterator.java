package org.apache.cassandra.io.sstable.format.test.iterators;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.test.TestTableReader;
import org.apache.cassandra.io.util.FileDataInput;

import java.io.IOException;
import java.util.Iterator;
import java.util.SortedSet;

/**
 * Created by jake on 9/3/14.
 */
public class NameFilteredIterator implements OnDiskAtomIterator
{
    private final ColumnFamily emptyColumnFamily;
    private final DecoratedKey key;
    private final SortedSet<CellName> names;
    private Iterator<OnDiskAtom> reader;
    private FileDataInput input;

    public NameFilteredIterator(TestTableReader sstable, FileDataInput input, DecoratedKey key, SortedSet<CellName> names, RowIndexEntry indexEntry)
    {
        this.key = key;
        this.names = names;
        this.input = input;
        emptyColumnFamily = ArrayBackedSortedColumns.factory.create(sstable.metadata);
        try
        {
            input.seek(indexEntry.position + indexEntry.headerOffset());

            emptyColumnFamily.delete(DeletionTime.serializer.deserialize(input));
        } catch (IOException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, sstable.getFilename());
        }


        reader = new TestTablePartitionIterator(input, sstable.metadata, names);
    }

    public NameFilteredIterator(TestTableReader sstable, DecoratedKey key, SortedSet<CellName> names)
    {
        this.key = key;
        this.names = names;
        emptyColumnFamily = ArrayBackedSortedColumns.factory.create(sstable.metadata);

        RowIndexEntry entry = sstable.getPosition(key, SSTableReader.Operator.EQ, true);
        try
        {
            if (entry != null)
            {
                input = sstable.getFileDataInput(entry.position + entry.headerOffset());
                emptyColumnFamily.delete(DeletionTime.serializer.deserialize(input));

                reader = new TestTablePartitionIterator(input, sstable.metadata, names);
            }
        } catch (IOException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, sstable.getFilename());
        }
    }

    @Override
    public ColumnFamily getColumnFamily()
    {
        return emptyColumnFamily;
    }

    @Override
    public DecoratedKey getKey()
    {
        return key;
    }

    @Override
    public void close() throws IOException
    {
        if (input != null)
            input.close();
    }

    @Override
    public boolean hasNext()
    {
        if (reader == null)
            return false;

        return reader.hasNext();
    }

    @Override
    public OnDiskAtom next()
    {
        return reader.next();
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}

