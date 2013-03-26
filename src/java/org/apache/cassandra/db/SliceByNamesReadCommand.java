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
package org.apache.cassandra.db;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SliceByNamesReadCommand extends ReadCommand
{
    static final SliceByNamesReadCommandSerializer serializer = new SliceByNamesReadCommandSerializer();

    public final NamesQueryFilter filter;

    public SliceByNamesReadCommand(String table, ByteBuffer key, String cfName, NamesQueryFilter filter)
    {
        super(table, key, cfName, Type.GET_BY_NAMES);
        this.filter = filter;
    }

    public ReadCommand copy()
    {
        ReadCommand readCommand= new SliceByNamesReadCommand(table, key, cfName, filter);
        readCommand.setDigestQuery(isDigestQuery());
        return readCommand;
    }

    public Row getRow(Table table) throws IOException
    {
        DecoratedKey dk = StorageService.getPartitioner().decorateKey(key);
        return table.getRow(new QueryFilter(dk, cfName, filter));
    }

    @Override
    public String toString()
    {
        return "SliceByNamesReadCommand(" +
               "table='" + table + '\'' +
               ", key=" + ByteBufferUtil.bytesToHex(key) +
               ", cfName='" + cfName + '\'' +
               ", filter=" + filter +
               ')';
    }

    public IDiskAtomFilter filter()
    {
        return filter;
    }
}

class SliceByNamesReadCommandSerializer implements IVersionedSerializer<ReadCommand>
{
    public void serialize(ReadCommand cmd, DataOutput out, int version) throws IOException
    {
        serialize(cmd, null, out, version);
    }

    public void serialize(ReadCommand cmd, ByteBuffer superColumn, DataOutput out, int version) throws IOException
    {
        SliceByNamesReadCommand command = (SliceByNamesReadCommand) cmd;
        out.writeBoolean(command.isDigestQuery());
        out.writeUTF(command.table);
        ByteBufferUtil.writeWithShortLength(command.key, out);

        if (version < MessagingService.VERSION_20)
            new QueryPath(command.cfName, superColumn).serialize(out);
        else
            out.writeUTF(command.cfName);

        CFMetaData metadata = Schema.instance.getCFMetaData(cmd.table, cmd.cfName);
        metadata.comparator.namesQueryFilterSerializer().serialize(command.filter, out, version);
    }

    public ReadCommand deserialize(DataInput in, int version) throws IOException
    {
        boolean isDigest = in.readBoolean();
        String table = in.readUTF();
        ByteBuffer key = ByteBufferUtil.readWithShortLength(in);

        String cfName;
        ByteBuffer sc = null;
        if (version < MessagingService.VERSION_20)
        {
            QueryPath path = QueryPath.deserialize(in);
            cfName = path.columnFamilyName;
            sc = path.superColumnName;
        }
        else
        {
            cfName = in.readUTF();
        }

        CFMetaData metadata = Schema.instance.getCFMetaData(table, cfName);
        ReadCommand command;
        if (version < MessagingService.VERSION_20)
        {
            CellNameType comparator = metadata.comparator;
            if (metadata.cfType == ColumnFamilyType.Super)
            {
                comparator = CellNames.simpleDenseType(metadata.comparator.subtype(sc == null ? 0 : 1));
            }

            IDiskAtomFilter filter = comparator.namesQueryFilterSerializer().deserialize(in, version);

            if (metadata.cfType == ColumnFamilyType.Super)
                filter = SuperColumns.fromSCFilter(metadata.comparator, sc, filter);

            // Due to SC compat, it's possible we get back a slice filter at this point
            if (filter instanceof NamesQueryFilter)
                command = new SliceByNamesReadCommand(table, key, cfName, (NamesQueryFilter)filter);
            else
                command = new SliceFromReadCommand(table, key, cfName, (SliceQueryFilter)filter);
        }
        else
        {
            NamesQueryFilter filter = metadata.comparator.namesQueryFilterSerializer().deserialize(in, version);
            command = new SliceByNamesReadCommand(table, key, cfName, filter);
        }

        command.setDigestQuery(isDigest);
        return command;
    }

    public long serializedSize(ReadCommand cmd, TypeSizes sizes, int version)
    {
        return serializedSize(cmd, null, sizes, version);
    }

    public long serializedSize(ReadCommand cmd, ByteBuffer superColumn, TypeSizes sizes, int version)
    {
        SliceByNamesReadCommand command = (SliceByNamesReadCommand) cmd;
        int size = sizes.sizeof(command.isDigestQuery());
        int keySize = command.key.remaining();

        size += sizes.sizeof(command.table);
        size += sizes.sizeof((short)keySize) + keySize;

        if (version < MessagingService.VERSION_20)
        {
            size += new QueryPath(command.cfName, superColumn).serializedSize(sizes);
        }
        else
        {
            size += sizes.sizeof(command.cfName);
        }

        CFMetaData metadata = Schema.instance.getCFMetaData(cmd.table, cmd.cfName);
        size += metadata.comparator.namesQueryFilterSerializer().serializedSize(command.filter, sizes, version);
        return size;
    }
}
