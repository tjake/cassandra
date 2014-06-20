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
package org.apache.cassandra.stress.cql3.generator;

import org.apache.cassandra.stress.generatedata.Distribution;
import org.apache.cassandra.stress.settings.OptionDistribution;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.MurmurHash;


import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Random;

public class GeneratorConfig implements Serializable
{
    public final long seed;
    public final Random random;

    public final int sizeMin;
    public final int sizeMax;
    public final Distribution popdistribution;
    long[] hash = new long[2];

    public GeneratorConfig(String seedStr)
    {
        this(seedStr, 1,256, OptionDistribution.get("uniform(0..1024)").get());
    }

    public GeneratorConfig(String seedStr, int sizeMin, int sizeMax, Distribution popdistribution)
    {

        assert sizeMin >= 1 && sizeMax > sizeMin;

        ByteBuffer buf = ByteBufferUtil.bytes(seedStr);
        MurmurHash.hash3_x64_128(buf, buf.position(), buf.remaining(), 0, hash);

        seed = hash[0];
        random = new Random(seed);

        this.sizeMin = sizeMin;
        this.sizeMax = sizeMax;
        this.popdistribution = popdistribution;
    }

    protected long nextPopSeed()
    {
        ByteBuffer buf = ByteBufferUtil.bytes(31L * (popdistribution.next() + seed));
        MurmurHash.hash3_x64_128(buf, buf.position(), buf.remaining(), 0, hash);

        return hash[0];
    }

    protected long nextPop()
    {
        return popdistribution.next();
    }

}
