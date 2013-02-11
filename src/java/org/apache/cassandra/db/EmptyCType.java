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
package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;

public class EmptyCType extends CompositeCType
{
    public static final EmptyCType instance = new EmptyCType();

    private EmptyCType()
    {
        super(Collections.<AbstractType<?>>emptyList());
    }

    public boolean isPacked()
    {
        return true;
    }

    @Override
    public AbstractType<?> asAbstractType()
    {
        throw new UnsupportedOperationException();
    }
}
