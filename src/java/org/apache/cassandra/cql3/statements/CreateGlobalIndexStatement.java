package org.apache.cassandra.cql3.statements;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.GlobalIndexDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.IndexName;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.transport.Event;

public class CreateGlobalIndexStatement extends SchemaAlteringStatement
{
    private static final Logger logger = LoggerFactory.getLogger(CreateIndexStatement.class);
    private final String indexName;
    private final IndexTarget.Raw rawTarget;
    private final List<ColumnIdentifier.Raw> denormalized;
    private final boolean ifNotExists;


    public CreateGlobalIndexStatement(CFName name,
                                      IndexName indexName,
                                      IndexTarget.Raw target,
                                      List<ColumnIdentifier.Raw> denormalized,
                                      boolean ifNotExists)
    {
        super(name);
        this.indexName = indexName.getIdx();
        this.rawTarget = target;
        this.denormalized = denormalized;
        this.ifNotExists = ifNotExists;
    }


    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.ALTER);
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        CFMetaData cfm = ThriftValidation.validateColumnFamily(keyspace(), columnFamily());
        if (cfm.isCounter())
            throw new InvalidRequestException("Global indexes are not supported on counter tables");

        IndexTarget target = rawTarget.prepare(cfm);
        ColumnDefinition cd = cfm.getColumnDefinition(target.column);

        if (cd == null)
            throw new InvalidRequestException("No column definition found for column " + target.column);

        if (!cd.isClusteringColumn())
        {
            throw new InvalidRequestException("Can only use clustering columns for Global Indexes");
        }

        // If the denormalized are specified, make sure that they are in the schema
        for (ColumnIdentifier.Raw dcolumn: denormalized)
        {
            ColumnIdentifier column = dcolumn.prepare(cfm);
            if (cfm.getColumnDefinition(column) == null)
                throw new InvalidRequestException("Must use defined columns for Global Indexes");
        }

        // 2i don't support static columns, so I'm not either
        if (cd.isStatic())
            throw new InvalidRequestException("Global indexes are not allowed on static columns");
    }

    private String createIndexName(IndexTarget target) {
        return columnFamily() + "_" + target.column.toString() + "_idx";
    }

    public boolean announceMigration(boolean isLocalOnly) throws RequestValidationException
    {
        CFMetaData cfm = Schema.instance.getCFMetaData(keyspace(), columnFamily()).copy();
        IndexTarget target = rawTarget.prepare(cfm);
        logger.debug("Adding Global Index on {} named {}", target.column, indexName);

        String indexName = this.indexName;
        if (indexName == null)
            indexName = createIndexName(target);

        Collection<ColumnIdentifier> identifiers = new ArrayList<>();
        for(ColumnIdentifier.Raw rawIdentifer: denormalized)
            identifiers.add(rawIdentifer.prepare(cfm));

        cfm.addGlobalIndex(new GlobalIndexDefinition(indexName, target.column, identifiers));
        MigrationManager.announceColumnFamilyUpdate(cfm, false, isLocalOnly);
        return true;
    }
    
    public Event.SchemaChange changeEvent()
    {
        // Creating an index is akin to updating the CF
        return new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TABLE, keyspace(), columnFamily());
    }
}
