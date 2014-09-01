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
package org.apache.cassandra.db.rows;

import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.cassandra.db.*;

import org.apache.cassandra.config.ColumnDefinition;

public class StaticRow extends AbstractRow
{
    private final DeletionTime deletion;
    private final RowDataBlock data;

    private final long maxLiveTimestamp;
    private final int nowInSec;

    private StaticRow(long maxLiveTimestamp, DeletionTime deletion, RowDataBlock data, int nowInSec)
    {
        this.maxLiveTimestamp = maxLiveTimestamp;
        this.deletion = deletion.takeAlias();
        this.data = data;
        this.nowInSec = nowInSec;
    }

    public Columns columns()
    {
        return data.columns();
    }

    public Cell getCell(ColumnDefinition c)
    {
        assert !c.isComplex();
        if (data.simpleData == null)
            return null;

        int idx = columns().simpleIdx(c, 0);
        if (idx < 0)
            return null;

        return SimpleRowDataBlock.reusableCell().setTo(data.simpleData.data, c, idx);
    }

    public Cell getCell(ColumnDefinition c, CellPath path)
    {
        assert c.isComplex();

        ComplexRowDataBlock dataBlock = data.complexData;
        if (dataBlock == null)
            return null;

        int idx = dataBlock.cellIdx(0, c, path);
        if (idx < 0)
            return null;

        return SimpleRowDataBlock.reusableCell().setTo(dataBlock.cellData(0), c, idx);
    }

    public Iterator<Cell> getCells(ColumnDefinition c)
    {
        assert c.isComplex();
        return ComplexRowDataBlock.reusableComplexCells().setTo(data.complexData, 0, c);
    }

    public boolean hasComplexDeletion()
    {
        return data.hasComplexDeletion(0);
    }

    public DeletionTime getDeletion(ColumnDefinition c)
    {
        assert c.isComplex();
        if (data.complexData == null)
            return DeletionTime.LIVE;

        int idx = data.complexData.complexDeletionIdx(0, c);
        return idx < 0
             ? DeletionTime.LIVE
             : ComplexRowDataBlock.complexDeletionCursor().setTo(data.complexData.complexDelTimes, idx);
    }

    public Iterator<Cell> iterator()
    {
        return RowDataBlock.reusableIterator().setTo(data, 0);
    }

    public Row takeAlias()
    {
        return this;
    }

    public Clustering clustering()
    {
        return Clustering.STATIC_CLUSTERING;
    }

    public LivenessInfo primaryKeyLivenessInfo()
    {
        return LivenessInfo.NONE;
    }

    public long maxLiveTimestamp()
    {
        return maxLiveTimestamp;
    }

    public DeletionTime deletion()
    {
        return deletion;
    }

    public int nowInSec()
    {
        return nowInSec;
    }

    public static Builder builder(Columns columns, boolean inOrderCells, int nowInSec, boolean isCounter)
    {
        return new Builder(columns, inOrderCells, nowInSec, isCounter);
    }

    public static class Builder extends RowDataBlock.Writer
    {
        private final RowDataBlock data;
        private DeletionTime deletion = DeletionTime.LIVE;
        private long maxLiveTimestamp;
        private final int nowInSec;

        public Builder(Columns columns, boolean inOrderCells, int nowInSec, boolean isCounter)
        {
            super(inOrderCells);
            this.data = new RowDataBlock(columns, 1, false, isCounter);
            this.nowInSec = nowInSec;
            updateWriter(data);
        }

        public void writeClusteringValue(ByteBuffer buffer)
        {
            throw new UnsupportedOperationException();
        }

        public void writePartitionKeyLivenessInfo(LivenessInfo info)
        {
            // Static rows are special and don't really have an existence unless they have live cells,
            // so we shouldn't have any partition key liveness info.
            assert info.equals(LivenessInfo.NONE);
        }

        public void writeRowDeletion(DeletionTime deletion)
        {
            this.deletion = deletion;
        }

        public void writeMaxLiveTimestamp(long maxLiveTimestamp)
        {
            this.maxLiveTimestamp = maxLiveTimestamp;
        }

        public StaticRow build()
        {
            return new StaticRow(maxLiveTimestamp, deletion, data, nowInSec);
        }
    }
}
