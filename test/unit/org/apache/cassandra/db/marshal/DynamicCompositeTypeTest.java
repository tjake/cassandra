/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;
import static org.junit.Assert.fail;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.utils.*;

public class DynamicCompositeTypeTest extends SchemaLoader
{
    private static final String cfName = "StandardDynamicComposite";

    private static final DynamicCompositeType comparator;
    static
    {
        Map<Byte, AbstractType<?>> aliases = new HashMap<Byte, AbstractType<?>>();
        aliases.put((byte)'b', BytesType.instance);
        aliases.put((byte)'t', TimeUUIDType.instance);
        comparator = DynamicCompositeType.getInstance(aliases);
    }

    private static final int UUID_COUNT = 3;
    private static final UUID[] uuids = new UUID[UUID_COUNT];
    static
    {
        for (int i = 0; i < UUID_COUNT; ++i)
            uuids[i] = UUIDGen.getTimeUUID();
    }

    @Test
    public void testEndOfComponent()
    {
        CellName[] cnames = {
            createDynamicCompositeKey("test1", uuids[0], -1, false),
            createDynamicCompositeKey("test1", uuids[1], 24, false),
            createDynamicCompositeKey("test1", uuids[1], 42, false),
            createDynamicCompositeKey("test1", uuids[1], 83, false),
            createDynamicCompositeKey("test1", uuids[2], -1, false),
            createDynamicCompositeKey("test1", uuids[2], 42, false),
        };

        CellName start = createDynamicCompositeKey("test1", uuids[1], -1, false);
        CellName stop = createDynamicCompositeKey("test1", uuids[1], -1, true);

        for (int i = 0; i < 1; ++i)
        {
            assert comparator.compare(start, cnames[i]) > 0;
            assert comparator.compare(stop, cnames[i]) > 0;
        }
        for (int i = 1; i < 4; ++i)
        {
            assert comparator.compare(start, cnames[i]) < 0;
            assert comparator.compare(stop, cnames[i]) > 0;
        }
        for (int i = 4; i < cnames.length; ++i)
        {
            assert comparator.compare(start, cnames[i]) < 0;
            assert comparator.compare(stop, cnames[i]) < 0;
        }
    }

    @Test
    public void testGetString()
    {
        String test1Hex = ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes("test1"));
        CellName key = createDynamicCompositeKey("test1", uuids[1], 42, false);
        assert comparator.getString(key.bb).equals("b@" + test1Hex + ":t@" + uuids[1] + ":IntegerType@42");

        key = createDynamicCompositeKey("test1", uuids[1], -1, true);
        assert comparator.getString(key.bb).equals("b@" + test1Hex + ":t@" + uuids[1] + ":!");
    }

    @Test
    public void testFromString()
    {
        String test1Hex = ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes("test1"));
        CellName key = createDynamicCompositeKey("test1", uuids[1], 42, false);
        assert key.equals(comparator.fromString("b@" + test1Hex + ":t@" + uuids[1] + ":IntegerType@42"));

        key = createDynamicCompositeKey("test1", uuids[1], -1, true);
        assert key.equals(comparator.fromString("b@" + test1Hex + ":t@" + uuids[1] + ":!"));
    }

    @Test
    public void testValidate()
    {
        CellName key = createDynamicCompositeKey("test1", uuids[1], 42, false);
        comparator.validate(key.bb);

        key = createDynamicCompositeKey("test1", null, -1, false);
        comparator.validate(key.bb);

        key = createDynamicCompositeKey("test1", uuids[2], -1, true);
        comparator.validate(key.bb);

        key.bb.get(); // make sure we're not aligned anymore
        try
        {
            comparator.validate(key.bb);
            fail("Should not validate");
        }
        catch (MarshalException e) {}

        key = CellName.wrap(ByteBuffer.allocate(5 + "test1".length() + 5 + 14));
        key.bb.putShort((short) (0x8000 | 'b'));
        key.bb.putShort((short) "test1".length());
        key.bb.put(ByteBufferUtil.bytes("test1"));
        key.bb.put((byte) 0);
        key.bb.putShort((short) (0x8000 | 't'));
        key.bb.putShort((short) 14);
        key.bb.rewind();
        try
        {
            comparator.validate(key.bb);
            fail("Should not validate");
        }
        catch (MarshalException e)
        {
            assert e.toString().contains("TimeUUID should be 16 or 0 bytes");
        }

        key = createDynamicCompositeKey("test1", UUID.randomUUID(), 42, false);
        try
        {
            comparator.validate(key.bb);
            fail("Should not validate");
        }
        catch (MarshalException e)
        {
            assert e.toString().contains("Invalid version for TimeUUID type");
        }
    }

    @Test
    public void testFullRound() throws Exception
    {
        Table table = Table.open("Keyspace1");
        ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);

        CellName cname1 = createDynamicCompositeKey("test1", null, -1, false);
        CellName cname2 = createDynamicCompositeKey("test1", uuids[0], 24, false);
        CellName cname3 = createDynamicCompositeKey("test1", uuids[0], 42, false);
        CellName cname4 = createDynamicCompositeKey("test2", uuids[0], -1, false);
        CellName cname5 = createDynamicCompositeKey("test2", uuids[1], 42, false);

        ByteBuffer key = ByteBufferUtil.bytes("k");
        RowMutation rm = new RowMutation("Keyspace1", key);
        addColumn(rm, cname5);
        addColumn(rm, cname1);
        addColumn(rm, cname4);
        addColumn(rm, cname2);
        addColumn(rm, cname3);
        rm.apply();

        ColumnFamily cf = cfs.getColumnFamily(QueryFilter.getIdentityFilter(Util.dk("k"), new QueryPath(cfName, null, null)));

        Iterator<IColumn> iter = cf.getSortedColumns().iterator();

        assert iter.next().name().equals(cname1);
        assert iter.next().name().equals(cname2);
        assert iter.next().name().equals(cname3);
        assert iter.next().name().equals(cname4);
        assert iter.next().name().equals(cname5);
    }

    @Test
    public void testUncomparableColumns()
    {
        CellName bytes = CellName.wrap(ByteBuffer.allocate(2 + 2 + 4 + 1));
        bytes.bb.putShort((short)(0x8000 | 'b'));
        bytes.bb.putShort((short) 4);
        bytes.bb.put(new byte[4]);
        bytes.bb.put((byte) 0);
        bytes.bb.rewind();

        CellName uuid = CellName.wrap(ByteBuffer.allocate(2 + 2 + 16 + 1));
        uuid.bb.putShort((short)(0x8000 | 't'));
        uuid.bb.putShort((short) 16);
        uuid.bb.put(UUIDGen.decompose(uuids[0]));
        uuid.bb.put((byte) 0);
        uuid.bb.rewind();

        try
        {
            int c = comparator.compare(bytes, uuid);
            assert c == -1 : "Expecting bytes to sort before uuid, but got " + c;
        }
        catch (Exception e)
        {
            fail("Shouldn't throw exception");
        }
    }

    public void testCompatibility() throws Exception
    {
        assert TypeParser.parse("DynamicCompositeType()").isCompatibleWith(TypeParser.parse("DynamicCompositeType()"));
        assert TypeParser.parse("DynamicCompositeType(a => IntegerType)").isCompatibleWith(TypeParser.parse("DynamicCompositeType()"));
        assert TypeParser.parse("DynamicCompositeType(b => BytesType, a => IntegerType)").isCompatibleWith(TypeParser.parse("DynamicCompositeType(a => IntegerType)"));

        assert !TypeParser.parse("DynamicCompositeType(a => BytesType)").isCompatibleWith(TypeParser.parse("DynamicCompositeType(a => AsciiType)"));
        assert !TypeParser.parse("DynamicCompositeType(a => BytesType)").isCompatibleWith(TypeParser.parse("DynamicCompositeType(a => BytesType, b => AsciiType)"));
    }

    private void addColumn(RowMutation rm, CellName cname)
    {
        rm.add(new QueryPath(cfName, null , cname.bb), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
    }

    private CellName createDynamicCompositeKey(String s, UUID uuid, int i, boolean lastIsOne)
    {
        ByteBuffer bytes = ByteBufferUtil.bytes(s);
        int totalSize = 0;
        if (s != null)
        {
            totalSize += 2 + 2 + bytes.remaining() + 1;
            if (uuid != null)
            {
                totalSize += 2 + 2 + 16 + 1;
                if (i != -1)
                {
                    totalSize += 2 + "IntegerType".length() + 2 + 1 + 1;
                }
            }
        }

        ByteBuffer bb = ByteBuffer.allocate(totalSize);

        if (s != null)
        {
            bb.putShort((short)(0x8000 | 'b'));
            bb.putShort((short) bytes.remaining());
            bb.put(bytes);
            bb.put(uuid == null && lastIsOne ? (byte)1 : (byte)0);
            if (uuid != null)
            {
                bb.putShort((short)(0x8000 | 't'));
                bb.putShort((short) 16);
                bb.put(UUIDGen.decompose(uuid));
                bb.put(i == -1 && lastIsOne ? (byte)1 : (byte)0);
                if (i != -1)
                {
                    bb.putShort((short) "IntegerType".length());
                    bb.put(ByteBufferUtil.bytes("IntegerType"));
                    // We are putting a byte only because our test use ints that fit in a byte *and* IntegerType.fromString() will
                    // return something compatible (i.e, putting a full int here would break 'fromStringTest')
                    bb.putShort((short) 1);
                    bb.put((byte)i);
                    bb.put(lastIsOne ? (byte)1 : (byte)0);
                }
            }
        }
        bb.rewind();
        return CellName.wrap(bb);
    }
}
