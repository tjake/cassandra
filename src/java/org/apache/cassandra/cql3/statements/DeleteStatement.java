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

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.thrift.ThriftValidation;

/**
 * A <code>DELETE</code> parsed from a CQL query statement.
 */
public class DeleteStatement extends ModificationStatement
{
    private CFDefinition cfDef;
    private final List<Operation.RawDeletion> deletions;
    private final List<Relation> whereClause;

    private final List<Operation> toRemove;
    private final Map<ColumnIdentifier, List<Term>> processedKeys = new HashMap<ColumnIdentifier, List<Term>>();

    public DeleteStatement(CFName name, List<Operation.RawDeletion> deletions, List<Relation> whereClause, Attributes attrs)
    {
        super(name, attrs);

        this.deletions = deletions;
        this.whereClause = whereClause;
        this.toRemove = new ArrayList<Operation>(deletions.size());
    }

    protected void validateConsistency(ConsistencyLevel cl) throws InvalidRequestException
    {
        if (type == Type.COUNTER)
            cl.validateCounterForWrite(cfDef.cfm);
        else
            cl.validateForWrite(cfDef.cfm.ksName);
    }

    public Collection<RowMutation> getMutationsInternal(List<ByteBuffer> variables, boolean local, ConsistencyLevel cl, long now, boolean isBatch)
    throws RequestExecutionException, RequestValidationException
    {
        // keys
        List<ByteBuffer> keys = UpdateStatement.buildKeyNames(cfDef, processedKeys, variables);

        // columns
        CBuilder builder = cfDef.cfm.comparator.builder();
        CFDefinition.Name firstEmpty = UpdateStatement.buildColumnNames(cfDef, processedKeys, builder, variables, false);
        Composite prefix = builder.build();

        if (prefix.size() < cfDef.columns.size() && !toRemove.isEmpty())
            throw new InvalidRequestException(String.format("Missing mandatory PRIMARY KEY part %s since %s specified", firstEmpty, toRemove.iterator().next().columnName));

        Set<ByteBuffer> toRead = null;
        for (Operation op : toRemove)
        {
            if (op.requiresRead())
            {
                if (toRead == null)
                    toRead = new TreeSet<ByteBuffer>(UTF8Type.instance);
                toRead.add(op.columnName.name.key);
            }
        }

        Map<ByteBuffer, CQL3Row> rows = toRead != null ? readRows(keys, prefix, toRead, cfDef.cfm, local, cl) : null;

        Collection<RowMutation> rowMutations = new ArrayList<RowMutation>(keys.size());
        UpdateParameters params = new UpdateParameters(cfDef.cfm, variables, getTimestamp(now), -1, rows);

        for (ByteBuffer key : keys)
            rowMutations.add(mutationForKey(cfDef, key, prefix, params, isBatch));

        return rowMutations;
    }

    public RowMutation mutationForKey(CFDefinition cfDef, ByteBuffer key, Composite prefix, UpdateParameters params, boolean isBatch)
    throws InvalidRequestException
    {
        QueryProcessor.validateKey(key);
        ColumnFamily cf = TreeMapBackedSortedColumns.factory.create(Schema.instance.getCFMetaData(cfDef.cfm.ksName, columnFamily()));

        if (toRemove.isEmpty())
        {
            // We delete the slice selected by the prefix.
            // However, for performance reasons, we distinguish 2 cases:
            //   - It's a full internal row delete
            //   - It's a full cell name (i.e it's a dense layout and the prefix is full)
            if (prefix.isEmpty())
            {
                // No columns specified, delete the row
                cf.delete(new DeletionInfo(params.timestamp, params.localDeletionTime));
            }
            else if (cfDef.cfm.comparator.isDense() && prefix.size() == cfDef.columns.size())
            {
                cf.addAtom(params.makeTombstone(CellNames.makeDense(prefix)));
            }
            else
            {
                cf.addAtom(params.makeRangeTombstone(prefix.slice()));
            }
        }
        else
        {
            for (Operation op : toRemove)
                op.execute(key, cf, prefix, params);
        }

        RowMutation rm;
        if (isBatch)
        {
            // we might group other mutations together with this one later, so make it mutable
            rm = new RowMutation(cfDef.cfm.ksName, key);
            rm.add(cf);
        }
        else
        {
            rm = new RowMutation(cfDef.cfm.ksName, key, cf);
        }
        return rm;
    }

    public ParsedStatement.Prepared prepare(ColumnSpecification[] boundNames) throws InvalidRequestException
    {
        CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace(), columnFamily());
        type = metadata.getDefaultValidator().isCommutative() ? Type.COUNTER : Type.LOGGED;

        cfDef = metadata.getCfDef();
        UpdateStatement.processKeys(cfDef, whereClause, processedKeys, boundNames);

        for (Operation.RawDeletion deletion : deletions)
        {
            CFDefinition.Name name = cfDef.get(deletion.affectedColumn());
            if (name == null)
                throw new InvalidRequestException(String.format("Unknown identifier %s", deletion.affectedColumn()));

            // For compact, we only have one value except the key, so the only form of DELETE that make sense is without a column
            // list. However, we support having the value name for coherence with the static/sparse case
            if (name.kind != CFDefinition.Name.Kind.COLUMN_METADATA && name.kind != CFDefinition.Name.Kind.VALUE_ALIAS)
                throw new InvalidRequestException(String.format("Invalid identifier %s for deletion (should not be a PRIMARY KEY part)", name));

            Operation op = deletion.prepare(name);
            op.collectMarkerSpecification(boundNames);
            toRemove.add(op);
        }

        return new ParsedStatement.Prepared(this, Arrays.<ColumnSpecification>asList(boundNames));
    }

    public ParsedStatement.Prepared prepare() throws InvalidRequestException
    {
        ColumnSpecification[] boundNames = new ColumnSpecification[getBoundsTerms()];
        return prepare(boundNames);
    }

    public String toString()
    {
        return String.format("DeleteStatement(name=%s, columns=%s, keys=%s)",
                             cfName,
                             deletions,
                             whereClause);
    }
}
