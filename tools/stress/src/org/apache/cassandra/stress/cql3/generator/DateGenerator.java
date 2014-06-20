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

import java.util.Date;

public class DateGenerator extends Generator<Date>
{
    long min = new Date(1970,1,1).getTime();
    long max = new Date(2999,1,1).getTime();

    public DateGenerator(GeneratorConfig config)
    {
        super(config);
    }

    @Override
    public Date generate(long workIndex)
    {
        long seed = config.nextPop();

        if (seed < min || seed > max)
            seed = min + (Math.abs(seed) % (max - min));

        return new Date(seed);
    }
}
