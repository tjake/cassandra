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

import java.util.Collections;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.functions.Function;

/**
 * Base class for <code>Conditions</code> classes.
 *
 */
abstract class AbstractConditions implements Conditions
{
    @Override
    public Iterable<Function> getFunctions()
    {
        return Collections.emptyList();
    }

    @Override
    public Iterable<ColumnDefinition> getColumns()
    {
        return null;
    }

    @Override
    public boolean isEmpty()
    {
        return false;
    }

    @Override
    public boolean applyToStaticColumns()
    {
        return false;
    }

    @Override
    public boolean applyToRegularColumns()
    {
        return false;
    }

    @Override
    public boolean isIfExists()
    {
        return false;
    }

    @Override
    public boolean isIfNotExists()
    {
        return false;
    }
}
