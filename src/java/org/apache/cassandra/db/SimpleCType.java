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
import java.util.List;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;

public class SimpleCType extends AbstractCType
{
    private final AbstractType<?> type;

    public SimpleCType(AbstractType<?> type)
    {
        this.type = type;
    }

    public boolean isPacked()
    {
        return true;
    }

    public int size()
    {
        return 1;
    }

    public AbstractType<?> subtype(int i)
    {
        if (i != 0)
            throw new IndexOutOfBoundsException();
        return type;
    }

    public Composite fromByteBuffer(ByteBuffer bytes)
    {
        return bytes.remaining() == 0 ? Composites.EMPTY : new SimpleComposite(bytes);
    }

    public CBuilder builder()
    {
        return new SimpleCBuilder();
    }

    public CType setSubtype(int position, AbstractType<?> newType)
    {
        if (position != 0)
            throw new IndexOutOfBoundsException();
        return new SimpleCType(newType);
    }

    // Use sparingly, it defeats the purpose
    public AbstractType<?> asAbstractType()
    {
        return type;
    }

    private class SimpleCBuilder implements CBuilder
    {
        private ByteBuffer value;

        public int size()
        {
            return value == null ? 0 : 1;
        }

        public boolean isFull()
        {
            return value != null;
        }

        public CBuilder add(ByteBuffer value)
        {
            if (this.value != null)
                throw new IllegalStateException();
            this.value = value;
            return this;
        }

        public CBuilder add(Object value)
        {
            return add(((AbstractType)type).decompose(value));
        }

        public Composite build()
        {
            return value == null ? Composites.EMPTY : new SimpleComposite(value);
        }

        public Composite buildWith(ByteBuffer value)
        {
            if (this.value != null)
                throw new IllegalStateException();

            return new SimpleComposite(value);
        }
    }
}
