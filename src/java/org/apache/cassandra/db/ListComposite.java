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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.utils.Allocator;

/**
 * A Composite backed by a list.
 */
public class ListComposite extends AbstractComposite
{
    // This is non-packed version of Composites.EMPTY.
    static final ListComposite EMPTY = new ListComposite(Collections.<ByteBuffer>emptyList());

    private final List<ByteBuffer> elements;

    // The caller of this constructor should not reuse the elements list after
    // this call. I.e. it must copied the list if it plan on reusing it.
    ListComposite(List<ByteBuffer> elements)
    {
        this.elements = elements;
    }

    public int size()
    {
        return elements.size();
    }

    public ByteBuffer get(int i)
    {
        return elements.get(i);
    }

    public boolean isPacked()
    {
        return false;
    }

    public Composite copy(Allocator allocator)
    {
        List<ByteBuffer> elementsCopy = new ArrayList<ByteBuffer>(elements.size());
        for (ByteBuffer elt : elements)
            elementsCopy.add(allocator.clone(elt));
        return new ListComposite(elementsCopy);
    }
}
