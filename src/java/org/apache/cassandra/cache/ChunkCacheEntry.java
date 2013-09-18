package org.apache.cassandra.cache;

import org.apache.cassandra.utils.ObjectSizes;
import org.apache.commons.lang.ArrayUtils;

import java.util.Iterator;
import java.util.LinkedList;

public class ChunkCacheEntry implements IMeasurableMemory
{
    public final byte[] uncompressedChunk;
    public final LinkedList<Long> stats;

    private static final long HOT_THRESHOLD = 1000 * 60;

    //Used by cache serializer only (package-local)
    ChunkCacheEntry(byte[] uncompressedChunk, LinkedList<Long> stats)
    {
        this.uncompressedChunk = uncompressedChunk;

        if (uncompressedChunk.length == 0)
        {
            this.stats = stats;
        }
        else
        {
            //This block is fully cached
            this.stats = null;
        }
    }

    public ChunkCacheEntry()
    {
        this.uncompressedChunk = ArrayUtils.EMPTY_BYTE_ARRAY;
        this.stats = new LinkedList<Long>();
    }

    public ChunkCacheEntry(byte[] uncompressedChunk, int validBytes)
    {
        this.uncompressedChunk = new byte[validBytes];
        System.arraycopy(uncompressedChunk,0,this.uncompressedChunk,0,validBytes);

        this.stats = null;
    }

    public void touch()
    {
        while (stats.size() >= 4)
            stats.removeLast();

        stats.addFirst(System.currentTimeMillis());
    }

    public boolean isHot()
    {
        if (stats == null)
            return true;

        if (stats.size() < 4)
            return false;

        Iterator<Long> it = stats.iterator();

        long now = System.currentTimeMillis();

        while(it.hasNext()) {

            long time = it.next();

            if ((now - time) > HOT_THRESHOLD)
                return false;
        }

        return true;
    }

    @Override
    public long memorySize()
    {
        return ObjectSizes.getReferenceSize() + ObjectSizes.getArraySize(uncompressedChunk) + ObjectSizes.measureDeep(stats);
    }
}
