package org.apache.cassandra.cql3.statements;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.IndexName;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.KeyspaceNotDefinedException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.messages.ResultMessage;

public class DropGlobalIndexStatement extends SchemaAlteringStatement
{
    public final String indexName;
    public final boolean ifExists;

    // initialized in announceMigration()
    private String indexedCF;

    public DropGlobalIndexStatement(IndexName indexName, boolean ifExists)
    {
        super(indexName.getCfName());
        this.indexName = indexName.getIdx();
        this.ifExists = ifExists;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        CFMetaData cfm = findIndexedCF();
        if (cfm == null)
            return;

        state.hasColumnFamilyAccess(cfm.ksName, cfm.cfName, Permission.ALTER);
    }

    public void validate(ClientState state)
    {
        // validated in findIndexedCf()
    }

    public Event.SchemaChange changeEvent()
    {
        // Dropping an index is akin to updating the CF
        return new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TABLE, keyspace(), columnFamily());
    }

    @Override
    public ResultMessage execute(QueryState state, QueryOptions options) throws RequestValidationException
    {
        announceMigration(false);
        return indexedCF == null ? null : new ResultMessage.SchemaChange(changeEvent());
    }

    public boolean announceMigration(boolean isLocalOnly) throws InvalidRequestException, ConfigurationException
    {
        CFMetaData cfm = findIndexedCF();
        if (cfm == null)
            return false;

        CFMetaData updatedCfm = updateCFMetadata(cfm);
        indexedCF = updatedCfm.cfName;
        MigrationManager.announceColumnFamilyUpdate(updatedCfm, false, isLocalOnly);
        return true;
    }

    private CFMetaData updateCFMetadata(CFMetaData cfm)
    {
        ColumnDefinition column = findIndexedColumn(cfm);
        assert column != null;
        CFMetaData cloned = cfm.copy();
        ColumnDefinition toChange = cloned.getColumnDefinition(column.name);
        assert toChange.getIndexName() != null && toChange.getIndexName().equals(indexName);
        toChange.setIndexName(null);
        toChange.setIndexType(null, null);
        return cloned;
    }

    private CFMetaData findIndexedCF() throws InvalidRequestException
    {
        KSMetaData ksm = Schema.instance.getKSMetaData(keyspace());
        if (ksm == null)
            throw new KeyspaceNotDefinedException("Keyspace " + keyspace() + " does not exist");
        for (CFMetaData cfm : ksm.cfMetaData().values())
        {
            if (findIndexedColumn(cfm) != null)
                return cfm;
        }

        if (ifExists)
            return null;
        else
            throw new InvalidRequestException("Index '" + indexName + "' could not be found in any of the tables of keyspace '" + keyspace() + '\'');
    }

    private ColumnDefinition findIndexedColumn(CFMetaData cfm)
    {
        for (ColumnDefinition column : cfm.allColumns())
        {
            if (column.getIndexType() != null && column.getIndexName() != null && column.getIndexName().equals(indexName))
                return column;
        }
        return null;
    }

    @Override
    public String columnFamily()
    {
        assert indexedCF != null;
        return indexedCF;
    }
}
