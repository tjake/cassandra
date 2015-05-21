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

package org.apache.cassandra.cql3;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.serializers.SimpleDateSerializer;
import org.apache.cassandra.serializers.TimeSerializer;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.utils.ByteBufferUtil;

public class MaterializedViewTest extends CQLTester
{
    int protocolVersion = 3;

    @BeforeClass
    public static void startup()
    {
        requireNetwork();
    }

    @Test
    public void testAllTypes() throws Throwable
    {
        String myType = createType("CREATE TYPE %s (a int, b uuid, c set<text>)");

        createTable("CREATE TABLE %s (" +
                    "k int PRIMARY KEY, " +
                    "asciival ascii, " +
                    "bigintval bigint, " +
                    "blobval blob, " +
                    "booleanval boolean, " +
                    "dateval date, " +
                    "decimalval decimal, " +
                    "doubleval double, " +
                    "floatval float, " +
                    "inetval inet, " +
                    "intval int, " +
                    "textval text, " +
                    "timeval time, " +
                    "timestampval timestamp, " +
                    "timeuuidval timeuuid, " +
                    "uuidval uuid," +
                    "varcharval varchar, " +
                    "varintval varint, " +
                    "listval list<int>, " +
                    "frozenlistval frozen<list<int>>, " +
                    "setval set<uuid>, " +
                    "frozensetval frozen<set<uuid>>, " +
                    "mapval map<ascii, int>," +
                    "frozenmapval frozen<map<ascii, int>>," +
                    "tupleval frozen<tuple<int, ascii, uuid>>," +
                    "udtval frozen<" + myType + ">)");


        CFMetaData metadata = currentTableMetadata();

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        for (ColumnDefinition def : metadata.regularColumns())
        {
            if (!def.type.isCollection())
                executeNet(protocolVersion, "CREATE MATERIALIZED VIEW "+keyspace()+".mv_"+def.name+" AS SELECT * FROM %s PRIMARY KEY ("+def.name+",k)");
        }

        // fromJson() can only be used when the receiver type is known
        assertInvalidMessage("fromJson() cannot be used in the selection clause", "SELECT fromJson(asciival) FROM %s", 0, 0);

        String func1 = createFunction(KEYSPACE, "int", "CREATE FUNCTION %s (a int) CALLED ON NULL INPUT RETURNS text LANGUAGE java AS $$ return a.toString(); $$");
        createFunctionOverload(func1, "int", "CREATE FUNCTION %s (a text) CALLED ON NULL INPUT RETURNS text LANGUAGE java AS $$ return new String(a); $$");


        // handle nulls
        executeNet(protocolVersion, "INSERT INTO %s (k, asciival) VALUES (?, fromJson(?))", 0, null);

        // ================ ascii ================
        executeNet(protocolVersion, "INSERT INTO %s (k, asciival) VALUES (?, fromJson(?))", 0, "\"ascii text\"");
        assertRows(execute("SELECT k, asciival FROM %s WHERE k = ?", 0), row(0, "ascii text"));

        executeNet(protocolVersion, "INSERT INTO %s (k, asciival) VALUES (?, fromJson(?))", 0, "\"ascii \\\" text\"");
        assertRows(execute("SELECT k, asciival FROM %s WHERE k = ?", 0), row(0, "ascii \" text"));

        // test that we can use fromJson() in other valid places in queries
        assertRows(execute("SELECT asciival FROM %s WHERE k = fromJson(?)", "0"), row("ascii \" text"));


        //Check the MV
        assertRows(execute("SELECT k, udtval from mv_asciival WHERE asciival = ?", "ascii text"));
        assertRows(execute("SELECT k, udtval from mv_asciival WHERE asciival = ?", "ascii \" text"), row(0, null));

        executeNet(protocolVersion, "UPDATE %s SET asciival = fromJson(?) WHERE k = fromJson(?)", "\"ascii \\\" text\"", "0");
        assertRows(execute("SELECT k, udtval from mv_asciival WHERE asciival = ?", "ascii \" text"), row(0, null));

        executeNet(protocolVersion, "DELETE FROM %s WHERE k = fromJson(?)", "0");
        assertRows(execute("SELECT k, asciival FROM %s WHERE k = ?", 0));
        assertRows(execute("SELECT k, udtval from mv_asciival WHERE asciival = ?", "ascii \" text"));


        executeNet(protocolVersion, "INSERT INTO %s (k, asciival) VALUES (?, fromJson(?))", 0, "\"ascii text\"");
        assertRows(execute("SELECT k, udtval from mv_asciival WHERE asciival = ?", "ascii text"), row(0, null));


        // ================ bigint ================
        executeNet(protocolVersion, "INSERT INTO %s (k, bigintval) VALUES (?, fromJson(?))", 0, "123123123123");
        assertRows(execute("SELECT k, bigintval FROM %s WHERE k = ?", 0), row(0, 123123123123L));

        assertRows(execute("SELECT k, asciival from mv_bigintval WHERE bigintval = ?", 123123123123L), row(0, "ascii text"));


        // ================ blob ================
        executeNet(protocolVersion, "INSERT INTO %s (k, blobval) VALUES (?, fromJson(?))", 0, "\"0x00000001\"");
        assertRows(execute("SELECT k, blobval FROM %s WHERE k = ?", 0), row(0, ByteBufferUtil.bytes(1)));

        assertRows(execute("SELECT k, asciival from mv_blobval WHERE blobval = ?", ByteBufferUtil.bytes(1)), row(0, "ascii text"));

        // ================ boolean ================
        executeNet(protocolVersion, "INSERT INTO %s (k, booleanval) VALUES (?, fromJson(?))", 0, "true");
        assertRows(execute("SELECT k, booleanval FROM %s WHERE k = ?", 0), row(0, true));

        assertRows(execute("SELECT k, asciival from mv_booleanval WHERE booleanval = ?", true), row(0, "ascii text"));


        executeNet(protocolVersion, "INSERT INTO %s (k, booleanval) VALUES (?, fromJson(?))", 0, "false");
        assertRows(execute("SELECT k, booleanval FROM %s WHERE k = ?", 0), row(0, false));



        // ================ date ================
        executeNet(protocolVersion, "INSERT INTO %s (k, dateval) VALUES (?, fromJson(?))", 0, "\"1987-03-23\"");
        assertRows(execute("SELECT k, dateval FROM %s WHERE k = ?", 0), row(0, SimpleDateSerializer.dateStringToDays("1987-03-23")));


        // ================ decimal ================
        executeNet(protocolVersion, "INSERT INTO %s (k, decimalval) VALUES (?, fromJson(?))", 0, "123123.123123");
        assertRows(execute("SELECT k, decimalval FROM %s WHERE k = ?", 0), row(0, new BigDecimal("123123.123123")));

        executeNet(protocolVersion, "INSERT INTO %s (k, decimalval) VALUES (?, fromJson(?))", 0, "123123");
        assertRows(execute("SELECT k, decimalval FROM %s WHERE k = ?", 0), row(0, new BigDecimal("123123")));

        // accept strings for numbers that cannot be represented as doubles
        executeNet(protocolVersion, "INSERT INTO %s (k, decimalval) VALUES (?, fromJson(?))", 0, "\"123123.123123\"");
        assertRows(execute("SELECT k, decimalval FROM %s WHERE k = ?", 0), row(0, new BigDecimal("123123.123123")));

        executeNet(protocolVersion, "INSERT INTO %s (k, decimalval) VALUES (?, fromJson(?))", 0, "\"-1.23E-12\"");
        assertRows(execute("SELECT k, decimalval FROM %s WHERE k = ?", 0), row(0, new BigDecimal("-1.23E-12")));

        // ================ double ================
        executeNet(protocolVersion, "INSERT INTO %s (k, doubleval) VALUES (?, fromJson(?))", 0, "123123.123123");
        assertRows(execute("SELECT k, doubleval FROM %s WHERE k = ?", 0), row(0, 123123.123123d));

        executeNet(protocolVersion, "INSERT INTO %s (k, doubleval) VALUES (?, fromJson(?))", 0, "123123");
        assertRows(execute("SELECT k, doubleval FROM %s WHERE k = ?", 0), row(0, 123123.0d));

        // strings are also accepted
        executeNet(protocolVersion, "INSERT INTO %s (k, doubleval) VALUES (?, fromJson(?))", 0, "\"123123\"");
        assertRows(execute("SELECT k, doubleval FROM %s WHERE k = ?", 0), row(0, 123123.0d));


        // ================ float ================
        executeNet(protocolVersion, "INSERT INTO %s (k, floatval) VALUES (?, fromJson(?))", 0, "123123.123123");
        assertRows(execute("SELECT k, floatval FROM %s WHERE k = ?", 0), row(0, 123123.123123f));

        executeNet(protocolVersion, "INSERT INTO %s (k, floatval) VALUES (?, fromJson(?))", 0, "123123");
        assertRows(execute("SELECT k, floatval FROM %s WHERE k = ?", 0), row(0, 123123.0f));

        // strings are also accepted
        executeNet(protocolVersion, "INSERT INTO %s (k, floatval) VALUES (?, fromJson(?))", 0, "\"123123.0\"");
        assertRows(execute("SELECT k, floatval FROM %s WHERE k = ?", 0), row(0, 123123.0f));


        // ================ inet ================
        executeNet(protocolVersion, "INSERT INTO %s (k, inetval) VALUES (?, fromJson(?))", 0, "\"127.0.0.1\"");
        assertRows(execute("SELECT k, inetval FROM %s WHERE k = ?", 0), row(0, InetAddress.getByName("127.0.0.1")));

        executeNet(protocolVersion, "INSERT INTO %s (k, inetval) VALUES (?, fromJson(?))", 0, "\"::1\"");
        assertRows(execute("SELECT k, inetval FROM %s WHERE k = ?", 0), row(0, InetAddress.getByName("::1")));


        // ================ int ================
        executeNet(protocolVersion,"INSERT INTO %s (k, intval) VALUES (?, fromJson(?))", 0, "123123");
        assertRows(execute("SELECT k, intval FROM %s WHERE k = ?", 0), row(0, 123123));

        // strings are also accepted
        executeNet(protocolVersion, "INSERT INTO %s (k, intval) VALUES (?, fromJson(?))", 0, "\"123123\"");
        assertRows(execute("SELECT k, intval FROM %s WHERE k = ?", 0), row(0, 123123));


        // ================ text (varchar) ================
        executeNet(protocolVersion, "INSERT INTO %s (k, textval) VALUES (?, fromJson(?))", 0, "\"\"");
        assertRows(execute("SELECT k, textval FROM %s WHERE k = ?", 0), row(0, ""));

        executeNet(protocolVersion, "INSERT INTO %s (k, textval) VALUES (?, fromJson(?))", 0, "\"abcd\"");
        assertRows(execute("SELECT k, textval FROM %s WHERE k = ?", 0), row(0, "abcd"));

        executeNet(protocolVersion, "INSERT INTO %s (k, textval) VALUES (?, fromJson(?))", 0, "\"some \\\" text\"");
        assertRows(execute("SELECT k, textval FROM %s WHERE k = ?", 0), row(0, "some \" text"));

        executeNet(protocolVersion, "INSERT INTO %s (k, textval) VALUES (?, fromJson(?))", 0, "\"\\u2013\"");
        assertRows(execute("SELECT k, textval FROM %s WHERE k = ?", 0), row(0, "\u2013"));


        // ================ time ================
        executeNet(protocolVersion, "INSERT INTO %s (k, timeval) VALUES (?, fromJson(?))", 0, "\"07:35:07.000111222\"");
        assertRows(execute("SELECT k, timeval FROM %s WHERE k = ?", 0), row(0, TimeSerializer.timeStringToLong("07:35:07.000111222")));


        // ================ timestamp ================
        executeNet(protocolVersion, "INSERT INTO %s (k, timestampval) VALUES (?, fromJson(?))", 0, "123123123123");
        assertRows(execute("SELECT k, timestampval FROM %s WHERE k = ?", 0), row(0, new Date(123123123123L)));

        executeNet(protocolVersion, "INSERT INTO %s (k, timestampval) VALUES (?, fromJson(?))", 0, "\"2014-01-01\"");
        assertRows(execute("SELECT k, timestampval FROM %s WHERE k = ?", 0), row(0, new SimpleDateFormat("y-M-d").parse("2014-01-01")));


        // ================ timeuuid ================
        executeNet(protocolVersion, "INSERT INTO %s (k, timeuuidval) VALUES (?, fromJson(?))", 0, "\"6bddc89a-5644-11e4-97fc-56847afe9799\"");
        assertRows(execute("SELECT k, timeuuidval FROM %s WHERE k = ?", 0), row(0, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")));

        executeNet(protocolVersion, "INSERT INTO %s (k, timeuuidval) VALUES (?, fromJson(?))", 0, "\"6BDDC89A-5644-11E4-97FC-56847AFE9799\"");
        assertRows(execute("SELECT k, timeuuidval FROM %s WHERE k = ?", 0), row(0, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")));


        // ================ uuidval ================
        executeNet(protocolVersion, "INSERT INTO %s (k, uuidval) VALUES (?, fromJson(?))", 0, "\"6bddc89a-5644-11e4-97fc-56847afe9799\"");
        assertRows(execute("SELECT k, uuidval FROM %s WHERE k = ?", 0), row(0, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")));

        executeNet(protocolVersion, "INSERT INTO %s (k, uuidval) VALUES (?, fromJson(?))", 0, "\"6BDDC89A-5644-11E4-97FC-56847AFE9799\"");
        assertRows(execute("SELECT k, uuidval FROM %s WHERE k = ?", 0), row(0, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")));


        // ================ varint ================
        executeNet(protocolVersion, "INSERT INTO %s (k, varintval) VALUES (?, fromJson(?))", 0, "123123123123");
        assertRows(execute("SELECT k, varintval FROM %s WHERE k = ?", 0), row(0, new BigInteger("123123123123")));

        // accept strings for numbers that cannot be represented as longs
        executeNet(protocolVersion, "INSERT INTO %s (k, varintval) VALUES (?, fromJson(?))", 0, "\"1234567890123456789012345678901234567890\"");
        assertRows(execute("SELECT k, varintval FROM %s WHERE k = ?", 0), row(0, new BigInteger("1234567890123456789012345678901234567890")));


        // ================ lists ================
        executeNet(protocolVersion, "INSERT INTO %s (k, listval) VALUES (?, fromJson(?))", 0, "[1, 2, 3]");
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0), row(0, list(1, 2, 3)));

        executeNet(protocolVersion, "INSERT INTO %s (k, listval) VALUES (?, fromJson(?))", 0, "[]");
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0), row(0, null));


        // frozen
        executeNet(protocolVersion, "INSERT INTO %s (k, frozenlistval) VALUES (?, fromJson(?))", 0, "[1, 2, 3]");
        assertRows(execute("SELECT k, frozenlistval FROM %s WHERE k = ?", 0), row(0, list(1, 2, 3)));

        // ================ sets ================
        executeNet(protocolVersion, "INSERT INTO %s (k, setval) VALUES (?, fromJson(?))",
                0, "[\"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]");
        assertRows(execute("SELECT k, setval FROM %s WHERE k = ?", 0),
                   row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))))
        );

        // duplicates are okay, just like in CQL
        executeNet(protocolVersion, "INSERT INTO %s (k, setval) VALUES (?, fromJson(?))",
                0, "[\"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]");
        assertRows(execute("SELECT k, setval FROM %s WHERE k = ?", 0),
                   row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))))
        );

        executeNet(protocolVersion, "INSERT INTO %s (k, setval) VALUES (?, fromJson(?))", 0, "[]");
        assertRows(execute("SELECT k, setval FROM %s WHERE k = ?", 0), row(0, null));


        // frozen
        executeNet(protocolVersion, "INSERT INTO %s (k, frozensetval) VALUES (?, fromJson(?))",
                0, "[\"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]");
        assertRows(execute("SELECT k, frozensetval FROM %s WHERE k = ?", 0),
                   row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))))
        );

        executeNet(protocolVersion, "INSERT INTO %s (k, frozensetval) VALUES (?, fromJson(?))",
                0, "[\"6bddc89a-5644-11e4-97fc-56847afe9799\", \"6bddc89a-5644-11e4-97fc-56847afe9798\"]");
        assertRows(execute("SELECT k, frozensetval FROM %s WHERE k = ?", 0),
                   row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))))
        );

        // ================ maps ================
        executeNet(protocolVersion, "INSERT INTO %s (k, mapval) VALUES (?, fromJson(?))", 0, "{\"a\": 1, \"b\": 2}");
        assertRows(execute("SELECT k, mapval FROM %s WHERE k = ?", 0), row(0, map("a", 1, "b", 2)));

        executeNet(protocolVersion, "INSERT INTO %s (k, mapval) VALUES (?, fromJson(?))", 0, "{}");
        assertRows(execute("SELECT k, mapval FROM %s WHERE k = ?", 0), row(0, null));


        // frozen
        executeNet(protocolVersion, "INSERT INTO %s (k, frozenmapval) VALUES (?, fromJson(?))", 0, "{\"a\": 1, \"b\": 2}");
        assertRows(execute("SELECT k, frozenmapval FROM %s WHERE k = ?", 0), row(0, map("a", 1, "b", 2)));

        executeNet(protocolVersion, "INSERT INTO %s (k, frozenmapval) VALUES (?, fromJson(?))", 0, "{\"b\": 2, \"a\": 1}");
        assertRows(execute("SELECT k, frozenmapval FROM %s WHERE k = ?", 0), row(0, map("a", 1, "b", 2)));

        // ================ tuples ================
        executeNet(protocolVersion, "INSERT INTO %s (k, tupleval) VALUES (?, fromJson(?))", 0, "[1, \"foobar\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]");
        assertRows(execute("SELECT k, tupleval FROM %s WHERE k = ?", 0),
                   row(0, tuple(1, "foobar", UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")))
        );

        executeNet(protocolVersion, "INSERT INTO %s (k, tupleval) VALUES (?, fromJson(?))", 0, "[1, null, \"6bddc89a-5644-11e4-97fc-56847afe9799\"]");
        assertRows(execute("SELECT k, tupleval FROM %s WHERE k = ?", 0),
                   row(0, tuple(1, null, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")))
        );


        // ================ UDTs ================
        executeNet(protocolVersion, "INSERT INTO %s (k, udtval) VALUES (?, fromJson(?))", 0, "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}");
        assertRows(execute("SELECT k, udtval.a, udtval.b, udtval.c FROM %s WHERE k = ?", 0),
                   row(0, 1, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"), set("bar", "foo"))
        );

        // order of fields shouldn't matter
        executeNet(protocolVersion, "INSERT INTO %s (k, udtval) VALUES (?, fromJson(?))", 0, "{\"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"a\": 1, \"c\": [\"foo\", \"bar\"]}");
        assertRows(execute("SELECT k, udtval.a, udtval.b, udtval.c FROM %s WHERE k = ?", 0),
                   row(0, 1, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"), set("bar", "foo"))
        );

        // test nulls
        executeNet(protocolVersion, "INSERT INTO %s (k, udtval) VALUES (?, fromJson(?))", 0, "{\"a\": null, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}");
        assertRows(execute("SELECT k, udtval.a, udtval.b, udtval.c FROM %s WHERE k = ?", 0),
                   row(0, null, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"), set("bar", "foo"))
        );

        // test missing fields
        executeNet(protocolVersion, "INSERT INTO %s (k, udtval) VALUES (?, fromJson(?))", 0, "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\"}");
        assertRows(execute("SELECT k, udtval.a, udtval.b, udtval.c FROM %s WHERE k = ?", 0),
                   row(0, 1, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"), null)
        );


    }
}
