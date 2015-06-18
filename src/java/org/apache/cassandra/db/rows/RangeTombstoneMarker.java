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

import java.util.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;

/**
 * A marker for a range tombsone bound.
 *
 * It can be either the opening or the closing of a range tombstone.
 * Each marker also include the associated deletion time.
 */
public interface RangeTombstoneMarker extends Unfiltered
{
    @Override
    public Slice.Bound clustering();
    public DeletionTime deletionTime();

    public void copyTo(RangeTombstoneMarker.Writer writer);

    public interface Writer extends Slice.Bound.Writer
    {
        public void writeMarkerDeletion(DeletionTime deletion);
        public void endOfMarker();
    }

    /**
     * Utility class to help merging range tombstone markers coming from multiple inputs (UnfilteredRowIterators).
     * <p>
     * The assumption that each individual input must validate and that we must preserve in the output is that every
     * open marker has a corresponding close marker with the exact same deletion info, and that there is no other range
     * tombstone marker between those open and close marker (of course, they could be rows in between). In other word,
     * for any {@code UnfilteredRowIterator}, you only ever have to remenber the last open marker (if any) to have the
     * full picture of what is deleted by range tombstones at any given point of iterating that iterator.
     * <p>
     * Note that this class can merge both forward and reverse iterators. To deal with reverse, we just reverse how we
     * deal with open and close markers (in forward order, we'll get open-close, open-close, ..., while in reverse we'll
     * get close-open, close-open, ...).
     */
    public static class Merger
    {
        private final CFMetaData metadata;
        private final UnfilteredRowIterators.MergeListener listener;
        private final DeletionTime partitionDeletion;
        private final boolean reversed;

        private Slice.Bound bound;
        private final RangeTombstoneMarker[] markers;

        // Stores for each iterator, what is the currently open marker
        private final DeletionTimeArray openMarkers;
        private final DeletionTimeArray.Cursor openMarkersCursor = new DeletionTimeArray.Cursor();

        // The index in openMarkers of the "biggest" marker. This is the last open marker
        // that has been returned for the merge.
        private int openMarker = -1;

        // As reusable marker to return the result
        private final ReusableRangeTombstoneMarker reusableMarker;

        public Merger(CFMetaData metadata, int size, DeletionTime partitionDeletion, boolean reversed, UnfilteredRowIterators.MergeListener listener)
        {
            this.metadata = metadata;
            this.listener = listener;
            this.partitionDeletion = partitionDeletion;
            this.reversed = reversed;

            this.markers = new RangeTombstoneMarker[size];
            this.openMarkers = new DeletionTimeArray(size);
            this.reusableMarker = new ReusableRangeTombstoneMarker(metadata.clusteringColumns().size());
        }

        public void clear()
        {
            Arrays.fill(markers, null);
        }

        public void add(int i, RangeTombstoneMarker marker)
        {
            bound = marker.clustering();
            markers[i] = marker;
        }

        public UnfilteredRowIterators.MergedUnfiltered merge(UnfilteredRowIterators.MergedUnfiltered merged)
        {
            if (bound.kind().isStart() != reversed)
                return mergeOpenMarkers(merged);
            else
                return mergeCloseMarkers(merged);
        }

        // The deletion time for the currently open marker of iterator i
        private DeletionTime dt(int i)
        {
            return openMarkersCursor.setTo(openMarkers, i);
        }

        private UnfilteredRowIterators.MergedUnfiltered mergeOpenMarkers(UnfilteredRowIterators.MergedUnfiltered merged)
        {
            int toReturn = -1;
            int previouslyOpen = openMarker;
            for (int i = 0; i < markers.length; i++)
            {
                RangeTombstoneMarker marker = markers[i];
                if (marker == null)
                    continue;

                // We can completely ignore any marker that is shadowed by a partition level deletion
                if (partitionDeletion.supersedes(marker.deletionTime()))
                    continue;

                // We have an open marker.
                DeletionTime dt = marker.deletionTime();
                openMarkers.set(i, dt);

                // It's present after merge if it's bigger than the currently open marker.
                if (openMarker < 0 || dt.supersedes(dt(openMarker)))
                    openMarker = toReturn = i;
            }

            if (toReturn < 0)
                return merged.setTo(null);

            // If we had an open marker in the output stream, we must close it before opening the new one
            if (previouslyOpen >= 0)
            {
                DeletionTime prev = dt(previouslyOpen).takeAlias();
                Slice.Bound closingBound = bound.invert();
                if (listener != null)
                    listener.onMergedRangeTombstoneMarkers(closingBound, prev, markers);
                merged.setTo(new SimpleRangeTombstoneMarker(closingBound, prev));
            }

            DeletionTime dt = dt(toReturn);
            if (listener != null)
                listener.onMergedRangeTombstoneMarkers(bound, dt, markers);

            return previouslyOpen >= 0
                 ? merged.setSecondTo(reusableMarker.setTo(bound, dt))
                 : merged.setTo(reusableMarker.setTo(bound, dt));
        }

        private UnfilteredRowIterators.MergedUnfiltered mergeCloseMarkers(UnfilteredRowIterators.MergedUnfiltered merged)
        {
            DeletionTime previouslyOpenDeletion = null;
            for (int i = 0; i < markers.length; i++)
            {
                RangeTombstoneMarker marker = markers[i];
                if (marker == null)
                    continue;

                if (i == openMarker)
                    previouslyOpenDeletion = dt(i).takeAlias();

                // Close the marker for this iterator
                openMarkers.clear(i);
            }

            // What we do now depends on whether we've closed the current open marker is. If we haven't, then
            // we can ignore any close we had (the corresponding opens had been ignored).
            // If we have closed the open marker, we need to issue a close for that marker. However, we also need
            // to find the next biggest open marker. If there is none, then we're good, but otherwise, on top
            // of closing the marker, we need to open that new biggest marker.
            if (previouslyOpenDeletion != null)
            {
                // We've cleaned openMarker so update to find the new biggest one
                updateOpenMarker();

                DeletionTime newOpenDeletion = openMarker >= 0 ? dt(openMarker).takeAlias() : null;

                // What could happen is that the new "biggest" open marker has actually the exact same deletion than the
                // previously open one. In that case, there is no point in closing to re-open the same thing.
                if (newOpenDeletion != null && newOpenDeletion.equals(previouslyOpenDeletion))
                    return merged;

                if (listener != null)
                    listener.onMergedRangeTombstoneMarkers(bound, previouslyOpenDeletion, markers);

                merged.setTo(reusableMarker.setTo(bound, previouslyOpenDeletion));

                if (openMarker >= 0)
                {
                    Slice.Bound openingBound = bound.invert();
                    if (listener != null)
                        listener.onMergedRangeTombstoneMarkers(openingBound, newOpenDeletion, markers);

                    merged.setSecondTo(new SimpleRangeTombstoneMarker(openingBound, newOpenDeletion));
                }
            }
            return merged;
        }

        public DeletionTime activeDeletion()
        {
            // Note that we'll only have an openMarker if it supersedes the partition deletion
            return openMarker < 0 ? partitionDeletion : openMarkersCursor.setTo(openMarkers, openMarker);
        }

        private void updateOpenMarker()
        {
            openMarker = -1;
            for (int i = 0; i < openMarkers.size(); i++)
            {
                if (openMarkers.isLive(i) && (openMarker < 0 || openMarkers.supersedes(i, openMarker)))
                    openMarker = i;
            }
        }
    }
}
