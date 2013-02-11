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

public class CompositeCType extends AbstractCType
{
    private final List<AbstractType<?>> types;

    // It's up to the caller to pass a list that is effectively immutable
    CompositeCType(List<AbstractType<?>> types)
    {
        this.types = types;
    }

    public boolean isPacked()
    {
        return false;
    }

    public int size()
    {
        return types.size();
    }

    public AbstractType<?> subtype(int i)
    {
        return types.get(i);
    }

    public Composite fromByteBuffer(ByteBuffer bb)
    {
        if (bb.remaining() == 0)
            return Composites.EMPTY;

        List<ByteBuffer> elements = new ArrayList<ByteBuffer>(size());
        Composite.EOC eoc = readCompositeElements(bb, elements);
        return new ListComposite(elements).withEOC(eoc);
    }

    public CBuilder builder()
    {
        return new CompositeCBuilder();
    }

    public CType setSubtype(int position, AbstractType<?> newType)
    {
        List<AbstractType<?>> newTypes = new ArrayList<AbstractType<?>>(types);
        newTypes.set(position, newType);
        return new CompositeCType(newTypes);
    }

    // Use sparingly, it defeats the purpose
    public AbstractType<?> asAbstractType()
    {
        return CompositeType.getInstance(types);
    }

    private class CompositeCBuilder implements CBuilder
    {
        private List<ByteBuffer> values = new ArrayList<ByteBuffer>(size());
        private boolean built;

        public int size()
        {
            return CompositeCType.this.size();
        }

        public boolean isFull()
        {
            return values.size() == size();
        }

        public CBuilder add(ByteBuffer value)
        {
            if (isDone())
                throw new IllegalStateException();
            values.add(value);
            return this;
        }

        public CBuilder add(Object value)
        {
            if (isDone())
                throw new IllegalStateException();
            values.add(((AbstractType)types.get(values.size())).decompose(value));
            return this;
        }

        private boolean isDone()
        {
            return isFull() || built;
        }

        public Composite build()
        {
            if (values.isEmpty())
                return ListComposite.EMPTY;

            // We don't allow to add more element to a builder that has been built.
            // This allows to avoid a list copy below.
            built = true;
            return new ListComposite(values);
        }

        public Composite buildWith(ByteBuffer value)
        {
            if (values.isEmpty())
                return new ListComposite(Collections.singletonList(value));

            List<ByteBuffer> newValues = new ArrayList<ByteBuffer>(values.size() + 1);
            newValues.addAll(values);
            newValues.add(value);
            return new ListComposite(newValues);
        }
    }
}
