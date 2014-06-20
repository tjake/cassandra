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


import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.stress.Operation;
import org.apache.cassandra.stress.cql3.generator.Generator;
import org.apache.cassandra.stress.util.JavaDriverClient;
import org.apache.cassandra.stress.util.ThriftClient;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.utils.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class Cql3Inserter extends Operation
{
    private final StressProfile profile;
    private final List<Generator> generators;
    private final PreparedStatement sth;
    private final ConsistencyLevel cl;
    private final Integer batchSize;
    private final BatchStatement.Type batchType;

    public Cql3Inserter(State state, long idx)
    {
        super(state, idx);
        profile = state.settings.schema.stressProfile;
        assert profile != null;


        Pair<PreparedStatement, List<Generator>> insert = profile.getGeneratorsForInsert(state.settings);
        sth = insert.left;
        generators = insert.right;

        cl = ConsistencyLevel.valueOf(state.settings.command.consistencyLevel.name());

        batchSize = state.settings.schema.batchSize;
        batchType = state.settings.schema.batchType;

    }

    @Override
    public void run(final ThriftClient client) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void run(SimpleClient client) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void run(JavaDriverClient client) throws IOException
    {
        timeWithRetry(new CqlIn(client));
    }

    class CqlIn implements RunOp
    {
        final JavaDriverClient client;
        CqlIn(JavaDriverClient client)
        {
            this.client = client;
        }

        @Override
        public boolean run() throws Exception
        {
            if (batchSize > 1)
            {
                BatchStatement batch = new BatchStatement(batchType);

                batch.setConsistencyLevel(JavaDriverClient.from(cl));

                for (int i = 0; i < batchSize; i++)
                {
                    Object[] vars = new Object[generators.size()];

                    for (int j = 0; j < vars.length; j++)
                        vars[j] = generators.get(j).generate(index);

                    batch.add(sth.bind(vars));
                }

                client.getSession().execute(batch);
            }
            else
            {
                List<Object> vars = new ArrayList<>();

                for (Generator gen : generators)
                    vars.add(gen.generate(index));

                client.executePrepared(sth, vars, cl);
            }

            return true;
        }

        @Override
        public String key()
        {
            return null;
        }

        @Override
        public int keyCount()
        {
            return batchSize;
        }
    }
}
