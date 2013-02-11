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

import org.apache.cassandra.cql3.CQL3Row;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ColumnToCollectionType;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.IVersionedSerializer;

public interface CellNameType extends CType
{
    // A CellNameType is "dense" if the CellName it types are "dense", i.e. if
    // those cell names don't have the CQL3 column name stored as one of the
    // component of the cell name.
    public boolean isDense();

    public int clusteringPrefixSize();

    public boolean hasCollections();
    public boolean supportCollections();

    public ColumnToCollectionType collectionType();

    public CellName make(Object... components);

    public CQL3Row.Builder CQL3RowBuilder();

    public CellName cellFromByteBuffer(ByteBuffer bb);

    public CellNameType addCollection(ByteBuffer columnName, CollectionType newCollection);

    @Override
    public CellNameType setSubtype(int position, AbstractType<?> newType);

    // Ultimately, this should be split into an IVersionedSerializer and an ISSTableSerializer
    public ISerializer<CellName> cellSerializer();

    public Comparator<Column> columnComparator();
    public Comparator<Column> columnReverseComparator();
    public Comparator<OnDiskAtom> onDiskAtomComparator();

    public ColumnSerializer columnSerializer();
    public OnDiskAtom.Serializer onDiskAtomSerializer();
    public IVersionedSerializer<NamesQueryFilter> namesQueryFilterSerializer();
    public IVersionedSerializer<IDiskAtomFilter> diskAtomFilterSerializer();
}
