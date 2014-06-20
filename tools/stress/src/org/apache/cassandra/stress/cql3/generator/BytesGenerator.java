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

import org.apache.cassandra.utils.ByteBufferUtil;

import java.nio.ByteBuffer;

public class BytesGenerator extends Generator<ByteBuffer>
{
    byte[] bytes;
    public BytesGenerator(GeneratorConfig config)
    {
        super(config);
        bytes = new byte[(int)config.sizeMax + 1];
    }

    @Override
    public ByteBuffer generate(long workIndex)
    {
        //Need to predictably generate the same string for the same position in the distribution...
        long seed = config.nextPopSeed();
        config.random.setSeed(seed);

        int size = config.sizeMin + (int)(config.random.nextDouble() * (config.sizeMax - config.sizeMin));
        config.random.setSeed(seed);

        //keep it ascii
        for (int i = 0; i < size; i++)
            bytes[i] = (byte)(config.random.nextDouble() * 127);

        return ByteBufferUtil.clone(ByteBuffer.wrap(bytes, 0, size));
    }
}