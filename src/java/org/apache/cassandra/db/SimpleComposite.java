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

import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.utils.Allocator;

/**
 * A not-really-composite singleton Composite.
 */
public class SimpleComposite extends AbstractComposite
{
    private final ByteBuffer element;

    SimpleComposite(ByteBuffer element)
    {
        this.element = element;
    }

    public int size()
    {
        return 1;
    }

    public ByteBuffer get(int i)
    {
        if (i != 0)
            throw new IndexOutOfBoundsException();

        return element;
    }

    public boolean isPacked()
    {
        return true;
    }

    public Composite copy(Allocator allocator)
    {
        return new SimpleComposite(allocator.clone(element));
    }
}
