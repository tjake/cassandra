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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.MaterializedViewDefinition;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.selection.RawSelector;
import org.apache.cassandra.cql3.selection.Selectable;
import org.apache.cassandra.db.view.MaterializedView;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.transport.Event;

public class CreateMaterializedViewStatement extends SchemaAlteringStatement
{
    private static final Logger logger = LoggerFactory.getLogger(CreateIndexStatement.class);
    private final SelectStatement.RawStatement select;
    private final List<ColumnIdentifier.Raw> primaryKeys;
    private final boolean ifNotExists;

    public CreateMaterializedViewStatement(CFName viewName,
                                           SelectStatement.RawStatement select,
                                           List<ColumnIdentifier.Raw> primaryKeys,
                                           boolean ifNotExists)
    {
        super(viewName);
        this.select = select;
        this.primaryKeys = primaryKeys;
        this.ifNotExists = ifNotExists;
    }


    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        select.prepareKeyspace(state);
        state.hasKeyspaceAccess(keyspace(), Permission.CREATE);
        state.hasColumnFamilyAccess(select.keyspace(), select.columnFamily(), Permission.SELECT);
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        // We do validation in announceMigration to reduce doubling up of work
    }

    public boolean announceMigration(boolean isLocalOnly) throws RequestValidationException
    {
        // We need to make sure that:
        //  - primary key includes all columns in base table's primary key
        //  - make sure that the select statement does not have anything other than columns
        //    and their names match the base table's names
        //  - make sure that primary key does not include any collections
        //  - make sure there is no where clause in the select statement
        //  - make sure there is not currently a table or view

        if (!select.whereClause.isEmpty())
            throw new InvalidRequestException("Cannot use where clause when defining a materialized view");
        if (select.parameters.allowFiltering)
            throw new InvalidRequestException("Cannot use 'ALLOW FILTERING' when defining a materialized view");
        if (select.parameters.isDistinct)
            throw new InvalidRequestException("Cannot use 'DISTINCT' when defining a materialized view");
        if (select.parameters.isJson)
            throw new InvalidRequestException("Cannot use 'JSON' when defining a materialized view");
        if (select.limit != null)
            throw new InvalidRequestException("Cannot use 'LIMIT' when defining a materialized view");

        CFName base = select.cfName;
        // We enforce the keyspace because if the RF is different, the logic to wait for a
        // specific replica would break
        if (!base.getKeyspace().equals(keyspace()))
            throw new InvalidRequestException("Cannot create a materialized view on a table in a separate keyspace");

        CFMetaData cfm = ThriftValidation.validateColumnFamily(base.getKeyspace(), base.getColumnFamily());
        if (cfm.isCounter())
            throw new InvalidRequestException("Global indexes are not supported on counter tables");

        Set<ColumnIdentifier> included = new HashSet<>();
        for (RawSelector selector: select.selectClause)
        {
            Selectable.Raw selectable = selector.selectable;
            if (selectable instanceof Selectable.WithFieldSelection.Raw)
                throw new InvalidRequestException("Cannot select out a part of type when defining a materialized view");
            if (selectable instanceof Selectable.WithFunction.Raw)
                throw new InvalidRequestException("Cannot use function when defining a materialized view");
            if (selectable instanceof Selectable.WritetimeOrTTL.Raw)
                throw new InvalidRequestException("Cannot use function when defining a materialized view");
            ColumnIdentifier identifier = (ColumnIdentifier) selectable.prepare(cfm);
            included.add(identifier);
            if (selector.alias != null)
                throw new InvalidRequestException(String.format("Cannot alias column '%s' as '%s' when defining a materialized view", identifier.toString(), selector.alias.toString()));
        }

        Set<ColumnIdentifier> primaryKeyCols = new HashSet<>();
        for (ColumnDefinition definition: cfm.partitionKeyColumns())
            primaryKeyCols.add(definition.name);
        for (ColumnDefinition definition: cfm.clusteringColumns())
            primaryKeyCols.add(definition.name);

        boolean isTarget = true;
        ColumnIdentifier target = null;
        List<ColumnIdentifier> clusteringColumns = new ArrayList<>();
        for (ColumnIdentifier.Raw raw: primaryKeys)
        {
            ColumnIdentifier identifier = raw.prepare(cfm);
            if (isTarget)
            {
                target = identifier;
                isTarget = false;
                primaryKeyCols.remove(identifier);
            }
            else
            {
                clusteringColumns.add(identifier);
                if (!primaryKeyCols.remove(identifier))
                    throw new InvalidRequestException(String.format("Cannot include non-primary key column '%s' in materialized view primary key", identifier));
            }
        }

        for (ColumnIdentifier primaryKeyCol: primaryKeyCols)
        {
            clusteringColumns.add(primaryKeyCol);
        }

        if (target == null)
            throw new InvalidRequestException("Must select at least a column for a Materialized View");

        MaterializedViewDefinition definition = new MaterializedViewDefinition(base.getColumnFamily(), columnFamily(), target, clusteringColumns, included);

        CFMetaData indexCf = MaterializedView.getCFMetaData(definition, cfm);
        MigrationManager.announceNewColumnFamily(indexCf, isLocalOnly);

        CFMetaData newCfm = cfm.copy();
        newCfm.addMaterializedView(definition);
        MigrationManager.announceColumnFamilyUpdate(newCfm, false, isLocalOnly);

        return true;
    }

    public Event.SchemaChange changeEvent()
    {
        return new Event.SchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.TABLE, keyspace(), columnFamily());
    }
}
