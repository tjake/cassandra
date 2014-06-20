/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.stress.cql3;


import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CreateKeyspaceStatement;
import org.apache.cassandra.exceptions.RequestValidationException;

import org.apache.cassandra.stress.cql3.generator.*;
import org.apache.cassandra.stress.generatedata.Distribution;
import org.apache.cassandra.stress.settings.OptionDistribution;
import org.apache.cassandra.stress.settings.StressSettings;
import org.apache.cassandra.stress.util.JavaDriverClient;
import org.apache.cassandra.utils.Pair;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.error.YAMLException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class StressProfile implements Serializable
{
    private String keyspaceCql;
    private String tableCql;
    private String seedStr;

    public String keyspaceName;
    public String tableName;
    public Map<String, GeneratorConfig> columnConfigs;
    public Map<String, String> queries;

    transient TableMetadata tableMetaData;

    transient PreparedStatement insertStatement;
    transient List<Generator>   insertGenerators;

    transient PreparedStatement queryStatement;
    transient List<Generator>   queryGenerator;

    private void init(StressYaml yaml) throws RequestValidationException
    {

        keyspaceName = yaml.keyspace;
        keyspaceCql = yaml.keyspace_definition;
        tableName = yaml.table;
        tableCql = yaml.table_definition;
        seedStr = yaml.seed;
        queries = yaml.queries;

        assert keyspaceName != null : "keyspace name is required in yaml file";
        assert tableName != null : "table name is required in yaml file";
        assert queries != null : "queries map is required in yaml file";

        if (keyspaceCql != null && keyspaceCql.length() > 0)
        {
            String name = ((CreateKeyspaceStatement) QueryProcessor.parseStatement(keyspaceCql)).keyspace();
            assert name.equalsIgnoreCase(keyspaceName) : "Name in keyspace_definition doesn't match keyspace property: '" + name + "' != '" + keyspaceName + "'";
        }
        else
        {
            keyspaceCql = null;
        }

        if (tableCql != null && tableCql.length() > 0)
        {
            String name = CFMetaData.compile(tableCql, keyspaceName).cfName;
            assert name.equalsIgnoreCase(tableName) : "Name in table_definition doesn't match table property: '" + name + "' != '" + tableName + "'";
        }
        else
        {
            tableCql = null;
        }

        columnConfigs = new HashMap<>();

        for (Map<String,Object> spec : yaml.columnspec)
        {
            String name = null;
            int min = 1;
            int max = 256;
            Distribution popDistribution = OptionDistribution.get("uniform(1..1024)").get();

            for (Map.Entry<String, Object> entry : spec.entrySet() )
            {
                String key = entry.getKey();

                if (key.equalsIgnoreCase("name"))
                {
                    assert entry.getValue() instanceof String;
                    name = (String)entry.getValue();
                    continue;
                }


                if (key.equalsIgnoreCase("distribution"))
                {
                    assert entry.getValue() instanceof String;
                    popDistribution = OptionDistribution.get((String)entry.getValue()).get();
                    continue;
                }

                if (key.equalsIgnoreCase("min"))
                {
                    assert entry.getValue() instanceof Integer;
                    min = (Integer)entry.getValue();
                    continue;
                }

                if (key.equalsIgnoreCase("max"))
                {
                    assert entry.getValue() instanceof Integer;
                    max = (Integer)entry.getValue();
                    continue;
                }

                System.err.println("encountered unknown column spec key: "+key+" for column");
            }

            if (name == null)
                throw new IllegalArgumentException("Missing name argument in column spec");

            GeneratorConfig config = new GeneratorConfig(yaml.seed, min, max, popDistribution);
            columnConfigs.put(name, config);
        }
    }

    Generator getGenerator(final DataType type, GeneratorConfig config)
    {
        switch (type.getName())
        {
            case ASCII:
            case TEXT:
            case VARCHAR:
                return new StringGenerator(config);
            case BIGINT:
            case COUNTER:
                return new LongGenerator(config);
            case BLOB:
                return new BytesGenerator(config);
            case BOOLEAN:
                return new BooleanGenerator(config);
            case DECIMAL:
            case DOUBLE:
                return new DoubleGenerator(config);
            case FLOAT:
                return new FloatGenerator(config);
            case INET:
                return new InetGenerator(config);
            case INT:
            case VARINT:
                return new IntegerGenerator(config);
            case TIMESTAMP:
                return new DateGenerator(config);
            case UUID:
                return new UUIDGenerator(config);
            case TIMEUUID:
                return new TimeUUIDGenerator(config);
            case SET:
                return new SetGenerator(getGenerator(type.getTypeArguments().get(0), config), config);
            case LIST:
                return new ListGenerator(getGenerator(type.getTypeArguments().get(0), config), config);
            default:
                throw new UnsupportedOperationException();
        }
    }

    public void maybeCreateSchema(StressSettings settings)
    {
        JavaDriverClient client = settings.getJavaDriverClient(false);

        if (keyspaceCql != null)
        {
            try
            {
                client.execute(keyspaceCql, org.apache.cassandra.db.ConsistencyLevel.ONE);
            }
            catch (AlreadyExistsException e)
            {

            }
        }

        client.execute("use "+keyspaceName, org.apache.cassandra.db.ConsistencyLevel.ONE);

        if (tableCql != null)
        {
            try
            {
                client.execute(tableCql, org.apache.cassandra.db.ConsistencyLevel.ONE);
            }
            catch (AlreadyExistsException e)
            {

            }

            System.out.println(String.format("Created schema. Sleeping %ss for propagation.", settings.node.nodes.size()));
            Uninterruptibles.sleepUninterruptibly(settings.node.nodes.size(), TimeUnit.SECONDS);
        }


        maybeLoadSchemaInfo(settings);
    }


    private void maybeLoadSchemaInfo(StressSettings settings)
    {
        if (tableMetaData == null)
        {
            JavaDriverClient client = settings.getJavaDriverClient();

            synchronized (client)
            {

                if (tableMetaData != null)
                    return;

                tableMetaData = client.getCluster()
                    .getMetadata()
                    .getKeyspace(keyspaceName)
                    .getTable(tableName);


                //Fill in missing column configs
                for (ColumnMetadata col : tableMetaData.getColumns())
                {
                    if (columnConfigs.containsKey(col.getName()))
                        continue;

                    columnConfigs.put(col.getName(), new GeneratorConfig(seedStr));
                }
            }
        }
    }


    public Pair<PreparedStatement, List<Generator>> getGeneratorsForQuery(StressSettings settings)
    {
        if (queryGenerator == null)
        {
            synchronized (this)
            {
                if (queryGenerator == null)
                {
                    maybeLoadSchemaInfo(settings);

                    JavaDriverClient client = settings.getJavaDriverClient();
                    String query = queries.get(settings.schema.queryName);

                    if (query == null)
                        throw new IllegalArgumentException("No query named "+settings.schema.queryName+" found in yaml");

                    PreparedStatement sth = client.prepare(query);
                    List<Generator> generators = new ArrayList<>();

                    for (ColumnDefinitions.Definition c : sth.getVariables())
                        generators.add(getGenerator(c.getType(), columnConfigs.get(c.getName())));

                    queryGenerator = generators;
                    queryStatement = sth;
                }
            }
        }

        return Pair.create(queryStatement,queryGenerator);
    }


    private boolean isCounterTable()
    {
        for (ColumnMetadata c : tableMetaData.getColumns())
        {
            if (c.getType().getName() == DataType.Name.COUNTER)
                return true;
        }

        return false;
    }

    public Pair<PreparedStatement,List<Generator>> getGeneratorsForInsert(StressSettings settings)
    {
        if (insertGenerators == null)
        {
            synchronized (this)
            {
                if (insertGenerators == null)
                {
                    maybeLoadSchemaInfo(settings);


                    boolean isCounterTable = isCounterTable();

                    Set<ColumnMetadata> keyColumns = Sets.newHashSet(tableMetaData.getPrimaryKey());

                    //Non PK Columns
                    List<Generator> generators = new ArrayList<>();

                    StringBuilder sb = new StringBuilder();

                    sb.append("UPDATE \"").append(tableName).append("\" SET ");

                    //PK Columns
                    List<Generator> predGenerators = new ArrayList<>();
                    StringBuilder pred = new StringBuilder();
                    pred.append(" WHERE ");

                    boolean firstCol = true;
                    boolean firstPred = true;
                    for (ColumnMetadata c : tableMetaData.getColumns())
                    {

                        if (keyColumns.contains(c))
                        {
                            if (firstPred)
                                firstPred = false;
                            else
                                pred.append(" AND ");

                            pred.append(c.getName()).append(" = ?");

                            //also add the generator for this col
                            predGenerators.add(getGenerator(c.getType(), columnConfigs.get(c.getName())));
                        }
                        else
                        {
                            if (firstCol)
                                firstCol = false;
                            else
                                sb.append(",");

                            sb.append(c.getName()).append(" = ");

                            switch (c.getType().getName())
                            {
                                case SET:
                                case LIST:
                                case COUNTER:
                                    sb.append(c.getName()).append(" + ?");
                                    break;
                                default:
                                    sb.append("?");
                                    break;
                            };

                            //also add the generator for this col
                            generators.add(getGenerator(c.getType(), columnConfigs.get(c.getName())));
                        }


                    }

                    //Put PK predicates at the end
                    sb.append(pred);
                    generators.addAll(predGenerators);

                    JavaDriverClient client = settings.getJavaDriverClient();
                    insertStatement = client.prepare(sb.toString());
                    insertGenerators = generators;
                }
            }
        }

        return Pair.create(insertStatement, insertGenerators);
    }

    public static StressProfile load(File file) throws IOError
    {
        try
        {
            byte[] profileBytes = Files.readAllBytes(Paths.get(file.toURI()));

            Constructor constructor = new Constructor(StressYaml.class);

            Yaml yaml = new Yaml(constructor);

            StressYaml profileYaml = yaml.loadAs(new ByteArrayInputStream(profileBytes), StressYaml.class);

            StressProfile profile = new StressProfile();
            profile.init(profileYaml);

            return profile;
        }
        catch (YAMLException | IOException | RequestValidationException e)
        {
            throw new IOError(e);
        }
    }
}
