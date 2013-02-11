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
 *
 * A CellName is a Composite, but for which, for the sake of CQL3, we
 * distinguish different parts: a CellName has first a number of clustering
 * components, followed by the CQL3 column name, and then possibly followed by
 * a collection element part.
 *
 * The clustering prefix can itself be composed of multiple component. It can
 * also be empty if the table has no clustering keys. In general, the CQL3
 * column name follows. However, some type of COMPACT STORAGE layout do not
 * store the CQL3 column name in the cell name and so this part can be null (we
 * call "dense" the cells whose name don't store the CQL3 column name).
 *
 * Lastly, if the cell is part of a CQL3 collection, we'll have a last
 * component (that will represent a UUID for lists, an element for sets and a
 * key for maps).
 */
public interface CellName extends Composite
{
    // Can be EMPTY!
    public Composite clusteringPrefix();

    // Can be null
    public ByteBuffer cql3ColumnName();

    // Can be null
    public ByteBuffer collectionElement();

    public boolean isSameCQL3RowThan(CellName other);

    public boolean isCollectionCell();

    // intern or copy
    // This defeats prefix sharing :(
    @Override
    public CellName copy(Allocator allocator);
}
