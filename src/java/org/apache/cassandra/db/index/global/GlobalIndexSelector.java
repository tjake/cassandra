package org.apache.cassandra.db.index.global;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.CFRowAdder;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.composites.CBuilder;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.marshal.CollectionType;

public abstract class GlobalIndexSelector
{
    public final ColumnDefinition columnDefinition;
    protected GlobalIndexSelector(ColumnDefinition columnDefinition)
    {
        this.columnDefinition = columnDefinition;
    }

    public static class Holder
    {
        private final GlobalIndexSelector partitionSelector;
        private final List<GlobalIndexSelector> clusteringSelectors;
        private final List<GlobalIndexSelector> regularSelectors;
        private final List<GlobalIndexSelector> staticSelectors;
        private ByteBuffer partitionKey;
        private ByteBuffer[] clusteringColumns;
        private ByteBuffer[] regularColumns;
        private ByteBuffer[] staticColumns;

        public Holder(GlobalIndexSelector partitionSelector,
                      List<GlobalIndexSelector> clusteringSelectors,
                      List<GlobalIndexSelector> regularSelectors,
                      List<GlobalIndexSelector> staticSelectors)
        {
            this.partitionSelector = partitionSelector;
            this.clusteringSelectors = clusteringSelectors;
            this.regularSelectors = regularSelectors;
            this.staticSelectors = staticSelectors;

            this.clusteringColumns = new ByteBuffer[clusteringSelectors.size()];
            this.regularColumns = new ByteBuffer[regularSelectors.size()];
            this.staticColumns = new ByteBuffer[staticSelectors.size()];
        }

        private boolean tryUpdate(CellName cellName, ByteBuffer key, ColumnFamily cf, List<GlobalIndexSelector> selectors, ByteBuffer[] columns)
        {
            for (int i = 0; i < selectors.size(); i++)
            {
                GlobalIndexSelector selector = selectors.get(i);
                if (selector.selects(cellName))
                {
                    columns[i] = selector.value(cellName, key, cf);
                    return true;
                }
            }
            return false;
        }

        private void updatePartitionKey(ByteBuffer key, List<GlobalIndexSelector> selectors, ByteBuffer[] columns)
        {
            for (int i = 0; i < selectors.size(); i++)
            {
                GlobalIndexSelector selector = selectors.get(i);
                if (selector.isPrimaryKey())
                {
                    columns[i] = selector.value(key);
                }
            }
        }

        public void updatePartitionKey(ByteBuffer key)
        {
            if (partitionSelector.isPrimaryKey())
            {
                partitionKey = partitionSelector.value(key);
            }
            updatePartitionKey(key, clusteringSelectors, clusteringColumns);
            updatePartitionKey(key, regularSelectors, regularColumns);
            updatePartitionKey(key, staticSelectors, staticColumns);
        }

        public void update(CellName cellName, ByteBuffer key, ColumnFamily cf)
        {
            if (partitionSelector.selects(cellName))
            {
                partitionKey = partitionSelector.value(cellName, key, cf);
            }
            else
            {
                if (tryUpdate(cellName, key, cf, clusteringSelectors, clusteringColumns))
                    return;
                if (tryUpdate(cellName, key, cf, regularSelectors, regularColumns))
                    return;
                tryUpdate(cellName, key, cf, staticSelectors, staticColumns);
            }
        }

        public Mutation getTombstoneMutation(ColumnFamilyStore indexCfs, long timestamp)
        {
            if (partitionKey == null)
                return null;

            for (ByteBuffer clusteringColumn : clusteringColumns)
            {
                if (clusteringColumn == null) return null;
            }

            Mutation mutation = new Mutation(indexCfs.metadata.ksName, partitionKey);
            ColumnFamily indexCf = mutation.addOrGet(indexCfs.metadata);
            CellNameType cellNameType = indexCfs.getComparator();
            CellName cellName;
            if (cellNameType.isCompound())
            {
                CBuilder builder = cellNameType.prefixBuilder();
                for (ByteBuffer prefix : clusteringColumns)
                    builder = builder.add(prefix);
                cellName = cellNameType.rowMarker(builder.build());
            }
            else
            {
                assert clusteringColumns.length == 1;
                cellName = cellNameType.cellFromByteBuffer(clusteringColumns[0]);
            }
            indexCf.addTombstone(cellName, 0, timestamp);
            return mutation;
        }

        public Mutation getMutation(ColumnFamilyStore indexCfs, long timestamp)
        {
            if (partitionKey == null)
                return null;

            for (ByteBuffer clusteringColumn : clusteringColumns)
            {
                if (clusteringColumn == null) return null;
            }

            Mutation mutation = new Mutation(indexCfs.metadata.ksName, partitionKey);
            ColumnFamily indexCf = mutation.addOrGet(indexCfs.metadata);
            CellNameType cellNameType = indexCfs.getComparator();
            Composite composite;
            if (cellNameType.isCompound())
            {
                CBuilder builder = cellNameType.prefixBuilder();
                for (ByteBuffer prefix : clusteringColumns)
                    builder = builder.add(prefix);
                composite = builder.build();
            }
            else
            {
                assert clusteringColumns.length == 1;
                composite = cellNameType.make(clusteringColumns[0]);
            }
            CFRowAdder cfRowAdder = new CFRowAdder(indexCf, composite, timestamp);
            for (int i = 0; i < regularColumns.length; i++)
                cfRowAdder.add(regularSelectors.get(i).columnDefinition.name.toString(), regularColumns[i]);
            for (int i = 0; i < staticColumns.length; i++)
                cfRowAdder.add(staticSelectors.get(i).columnDefinition.name.toString(), staticColumns[i]);
            return mutation;
        }
    }

    public static GlobalIndexSelector create(ColumnFamilyStore baseCfs, ColumnDefinition cfDef)
    {
        if (cfDef.type.isCollection() && cfDef.type.isMultiCell())
        {
            switch (((CollectionType)cfDef.type).kind)
            {
                case LIST:
                    return new GlobalIndexSelectorOnList(cfDef);
                case SET:
                    return new GlobalIndexSelectorOnSet(cfDef);
                case MAP:
                    return new GlobalIndexSelectorOnMap(cfDef);
            }
        }

        switch (cfDef.kind)
        {
            case CLUSTERING_COLUMN:
                return new GlobalIndexSelectorOnClusteringColumn(cfDef);
            case REGULAR:
                return new GlobalIndexSelectorOnRegularColumn(baseCfs, cfDef);
            case PARTITION_KEY:
                return new GlobalIndexSelectorOnPartitionKey(baseCfs, cfDef);
        }
        throw new AssertionError();
    }

    /**
     * Depending on whether this column can overwrite the values of a different
     * @return True if a check for tombstones needs to be done, false otherwise
     */
    public abstract boolean canGenerateTombstones();

    public boolean isPrimaryKey()
    {
        return false;
    }

    public abstract boolean selects(CellName cellName);

    public abstract ByteBuffer value(CellName cellName, ByteBuffer key, ColumnFamily cf);

    public ByteBuffer value(ByteBuffer key)
    {
        throw new AssertionError("Cannot create a value from partition key");
    }
}
