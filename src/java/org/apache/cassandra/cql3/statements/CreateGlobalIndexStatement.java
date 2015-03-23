/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import org.apache.cassandra.db.index.GlobalIndex;
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
    private final List<ColumnIdentifier.Raw> included;
    private final boolean ifNotExists;


    public CreateGlobalIndexStatement(CFName name,
                                      IndexName indexName,
                                      IndexTarget.Raw target,
                                      List<ColumnIdentifier.Raw> included,
                                      boolean ifNotExists)
    {
        super(name);
        this.indexName = indexName.getIdx();
        this.rawTarget = target;
        this.included = included;
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

        // If the included are specified, make sure that they are in the schema
        for (ColumnIdentifier.Raw dcolumn: included)
        {
            ColumnIdentifier column = dcolumn.prepare(cfm);
            if (cfm.getColumnDefinition(column) == null)
                throw new InvalidRequestException("Must use defined columns for Global Indexes");
        }

        if (cd.isIndexed())
            throw new InvalidRequestException("Cannot create global index on a column which already has an index");

        // 2i don't support static columns, so I'm not either
        if (cd.isStatic())
            throw new InvalidRequestException("Global indexes are not allowed on static columns");

        if (!ifNotExists)
        {
            for (GlobalIndexDefinition definition : cfm.getGlobalIndexes().values())
            {
                if (definition.target.bytes.compareTo(cd.name.bytes) == 0)
                    throw new InvalidRequestException("GlobalIndex " + definition.indexName + " has already been defined for column " + target.column);
            }
        }
    }

    private String createIndexName(IndexTarget target) {
        return columnFamily() + "_" + target.column.toString() + "_idx";
    }

    public boolean announceMigration(boolean isLocalOnly) throws RequestValidationException
    {
        CFMetaData cfm = Schema.instance.getCFMetaData(keyspace(), columnFamily()).copy();
        IndexTarget target = rawTarget.prepare(cfm);
        if (!ifNotExists)
        {
            for (GlobalIndexDefinition definition : cfm.getGlobalIndexes().values())
            {
                if (definition.target.bytes.compareTo(target.column.bytes) == 0)
                    throw new InvalidRequestException("GlobalIndex " + definition.indexName + " has already been defined for column " + target.column);
            }
        }

        logger.debug("Adding Global Index on {} named {}", target.column, indexName);

        String indexName = this.indexName;
        if (indexName == null)
            indexName = createIndexName(target);

        Collection<ColumnIdentifier> identifiers = new ArrayList<>();
        for (ColumnIdentifier.Raw identifier: included)
            identifiers.add(identifier.prepare(cfm));

        GlobalIndexDefinition definition = new GlobalIndexDefinition(columnFamily(), indexName, target.column, identifiers);

        CFMetaData indexCf = GlobalIndex.getCFMetaData(definition, cfm);
        MigrationManager.announceNewColumnFamily(indexCf, isLocalOnly);

        cfm.addGlobalIndex(definition);
        MigrationManager.announceColumnFamilyUpdate(cfm, false, isLocalOnly);

        return true;
    }
    
    public Event.SchemaChange changeEvent()
    {
        // Creating an index is akin to updating the CF
        return new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TABLE, keyspace(), columnFamily());
    }
}
