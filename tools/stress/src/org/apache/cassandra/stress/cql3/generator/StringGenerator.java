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

public class StringGenerator extends Generator<String>
{
    private int[] codePoints;

    public StringGenerator(GeneratorConfig config)
    {
        super(config);
        codePoints = new int[config.sizeMax + 1];
    }


    @Override
    public String generate(long workIndex)
    {
        //Need to predictably generate the same string for the same position in the distribution...
        long seed = config.nextPop();
        config.random.setSeed(seed);

        int size = config.sizeMin + (int)(config.random.nextDouble() * (config.sizeMax - config.sizeMin));
        config.random.setSeed(seed);

        //keep it ascii
        for (int i = 0; i < size; i++)
            codePoints[i] = 32 + (int)(config.random.nextDouble() * (126 - 32));

        return new String(codePoints, 0, size);
    }
}
