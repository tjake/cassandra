package org.apache.cassandra.cql3.statements;

import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.cql3.IndexName;
import org.apache.cassandra.cql3.selection.RawSelector;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.Event;

import java.util.List;

public class CreateGlobalIndexStatement extends SchemaAlteringStatement
{

    private final String indexName;
    private final IndexTarget.Raw rawTarget;
    private final List<RawSelector> denormalized;
    private final boolean ifNotExists;


    public CreateGlobalIndexStatement(CFName name,
                                IndexName indexName,
                                IndexTarget.Raw target,
                                List<RawSelector> denormalized,
                                boolean ifNotExists)
    {
        super(name);
        this.indexName = indexName.getIdx();
        this.rawTarget = target;
        this.denormalized = denormalized;
        this.ifNotExists = ifNotExists;
    }


    @Override
    public Event.SchemaChange changeEvent()
    {
        // Creating an index is akin to updating the CF
        return new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TABLE, keyspace(), columnFamily());
    }

    @Override
    public boolean announceMigration(boolean isLocalOnly) throws RequestValidationException
    {
        if (isLocalOnly)
        {

        }
        return false;
    }

    @Override
    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {

    }

    @Override
    public void validate(ClientState state) throws RequestValidationException
    {

    }
}
