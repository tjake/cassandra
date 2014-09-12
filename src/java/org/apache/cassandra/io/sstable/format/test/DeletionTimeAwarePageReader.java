package org.apache.cassandra.io.sstable.format.test;

import org.apache.cassandra.db.DeletionTime;
import parquet.column.page.PageReadStore;

import java.util.Iterator;

/**
 * Created by jake on 8/25/14.
 */
public interface DeletionTimeAwarePageReader extends Iterator<PageReadStore>
{
    DeletionTime getDeletionTime();
}
