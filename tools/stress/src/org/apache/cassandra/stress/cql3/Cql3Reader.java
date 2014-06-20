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


import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
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

public class Cql3Reader extends Operation
{
    private final StressProfile profile;

    private final List<Generator> generators;
    private final PreparedStatement sth;
    private final ConsistencyLevel cl;

    public Cql3Reader(State state, long idx)
    {
        super(state, idx);
        profile = state.settings.schema.stressProfile;
        assert profile != null;

        if (state.settings.schema.queryName == null)
            throw new RuntimeException("Missing query name in program argument");

        Pair<PreparedStatement, List<Generator>> query = profile.getGeneratorsForQuery(state.settings);

        sth = query.left;
        generators = query.right;
        cl = ConsistencyLevel.valueOf(state.settings.command.consistencyLevel.name());
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
        timeWithRetry(new CqlOut(client));
    }

    class CqlOut implements RunOp
    {
        final JavaDriverClient client;
        int keyCount = 0;

        CqlOut(JavaDriverClient client)
        {
            this.client = client;
        }

        @Override
        public boolean run() throws Exception
        {

            List<Object> vars = new ArrayList<>();
            for (Generator gen : generators)
                vars.add(gen.generate(index));

            ResultSet rs = client.executePrepared(sth, vars, cl);

            keyCount = rs.all().size();
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
            return keyCount;
        }
    }
}
