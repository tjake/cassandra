package org.apache.cassandra.cache;

import com.google.common.primitives.Longs;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.Pair;

public class ChunkCacheKey implements CacheKey, Comparable<ChunkCacheKey>{

    private final String filePath;
    private final long offset;

    public ChunkCacheKey(String filePath, CompressionMetadata.Chunk chunk) {
        assert filePath != null && chunk != null;

        this.filePath = filePath;
        this.offset = chunk.offset;
    }

    @Override
    public Pair<String, String> getPathInfo() {
        return null;
    }

    @Override
    public long memorySize() {
        return ObjectSizes.getReferenceSize() + ObjectSizes.getReferenceSize() + ObjectSizes.getArraySize(filePath.getBytes()) + Longs.BYTES;
    }

    @Override
    public int compareTo(ChunkCacheKey key) {
        int r = filePath.compareTo(key.filePath);

        if (r == 0)
            r = Longs.compare(offset,key.offset);

        return r;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ChunkCacheKey that = (ChunkCacheKey) o;

        if (offset != that.offset) return false;
        if (filePath != null ? !filePath.equals(that.filePath) : that.filePath != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = filePath != null ? filePath.hashCode() : 0;
        result = 31 * result + (int) (offset ^ (offset >>> 32));
        return result;
    }
}
