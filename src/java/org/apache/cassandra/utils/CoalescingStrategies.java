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
package org.apache.cassandra.utils;

import org.slf4j.Logger;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import com.google.common.annotations.VisibleForTesting;

public class CoalescingStrategies
{

    @VisibleForTesting
    interface Clock
    {
        long nanoTime();
    }

    @VisibleForTesting
    static Clock CLOCK = new Clock()
    {
        public long nanoTime()
        {
            return System.nanoTime();
        }
    };

    public static interface Coalescable {
        long timestampNanos();
    }

    @VisibleForTesting
    static void parkLoop(long nanos)
    {
        long now = System.nanoTime();
        final long timer = now + nanos;
        do
        {
            LockSupport.parkNanos(timer - now);
        }
        while (timer - (now = System.nanoTime()) > nanos / 16);
    }

    private static boolean maybeSleep(int messages, long averageGap, long maxCoalesceWindow, Parker parker)
    {
        // only sleep if we can expect to double the number of messages we're sending in the time interval
        long sleep = messages * averageGap;
        if (sleep > maxCoalesceWindow)
            return false;

        // assume we receive as many messages as we expect; apply the same logic to the future batch:
        // expect twice as many messages to consider sleeping for "another" interval; this basically translates
        // to doubling our sleep period until we exceed our max sleep window
        while (sleep * 2 < maxCoalesceWindow)
            sleep *= 2;
        parker.park(sleep);
        return true;
    }

    public static abstract class CoalescingStrategy
    {
        protected final Parker parker;
        protected final Logger logger;
        protected final boolean DEBUG_COALESCING;

        protected CoalescingStrategy(Parker parker, Logger logger, boolean debug)
        {
            this.parker = parker;
            this.logger = logger;
            DEBUG_COALESCING = debug;
        }

        /*
         * Drain from the input blocking queue to the output list up to outSize elements
         *
         * The coalescing strategy may choose to park the current thread if it thinks it will
         * be able to produce an output list with more elements
         */
        public abstract void coalesce(BlockingQueue<Coalescable> input, List<Coalescable> out, int outSize) throws InterruptedException;
    }

    @VisibleForTesting
    interface Parker
    {
        void park(long nanos);
    }

    private static final Parker PARKER = new Parker()
    {
        @Override
        public void park(long nanos)
        {
            parkLoop(nanos);
        }
    };


    @VisibleForTesting
    static class TimeHorizonMovingAverageCoalescingStrategy extends CoalescingStrategy
    {
        // for now we'll just use 64ms per bucket; this can be made configurable, but results in ~1s for 16 samples
        private static final int INDEX_SHIFT = 26;
        private static final long BUCKET_INTERVAL = 1L << 26;
        private static final int BUCKET_COUNT = 16;
        private static final long INTERVAL = BUCKET_INTERVAL * BUCKET_COUNT;
        private static final long MEASURED_INTERVAL = BUCKET_INTERVAL * (BUCKET_COUNT - 1);

        // the minimum timestamp we will now accept updates for; only moves forwards, never backwards
        private long epoch = CLOCK.nanoTime();
        // the buckets, each following on from epoch; the measurements run from ix(epoch) to ix(epoch - 1)
        // ix(epoch-1) is a partial result, that is never actually part of the calculation, and most updates
        // are expected to hit this bucket
        private final int samples[] = new int[BUCKET_COUNT];
        private long sum = 0;
        private final long maxCoalesceWindow;

        public TimeHorizonMovingAverageCoalescingStrategy(int maxCoalesceWindow, Parker parker, Logger logger, boolean debug)
        {
            super(parker, logger, debug);
            this.maxCoalesceWindow = TimeUnit.MICROSECONDS.toNanos(maxCoalesceWindow);
            sum = 0;
            if (DEBUG_COALESCING) {
                new Thread() {
                    @Override
                    public void run() {
                        while (true) {
                            try
                            {
                                Thread.sleep(5000);
                            }
                            catch (InterruptedException e)
                            {
                                throw new AssertionError();
                            }
                            shouldLogAverage = true;
                        }
                    }
                }.start();
            }
        }

        private void logSample(long nanos)
        {
            long epoch = this.epoch;
            long delta = nanos - epoch;
            if (delta < 0)
                // have to simply ignore, but would be a bit crazy to get such reordering
                return;

            if (delta > INTERVAL)
                epoch = rollepoch(delta, epoch, nanos);

            int ix = ix(nanos);
            samples[ix]++;

            // if we've updated an old bucket, we need to update the sum to match
            if (ix != ix(epoch - 1))
                sum++;
        }

        private long averageGap()
        {
            if (sum == 0)
                return Integer.MAX_VALUE;
            return MEASURED_INTERVAL / sum;
        }

        // this sample extends past the end of the range we cover, so rollover
        private long rollepoch(long delta, long epoch, long nanos)
        {
            if (delta > 2 * INTERVAL)
            {
                // this sample is more than twice our interval ahead, so just clear our counters completely
                epoch = epoch(nanos);
                sum = 0;
                Arrays.fill(samples, 0);
            }
            else
            {
                // ix(epoch - 1) => last index; this is our partial result bucket, so we add this to the sum
                sum += samples[ix(epoch - 1)];
                // then we roll forwards, clearing buckets, until our interval covers the new sample time
                while (epoch + INTERVAL < nanos)
                {
                    int index = ix(epoch);
                    sum -= samples[index];
                    samples[index] = 0;
                    epoch += BUCKET_INTERVAL;
                }
            }
            // store the new epoch
            this.epoch = epoch;
            return epoch;
        }

        private long epoch(long latestNanos)
        {
            return (latestNanos - MEASURED_INTERVAL) & ~(BUCKET_INTERVAL - 1);
        }

        private int ix(long nanos)
        {
            return (int) ((nanos >>> INDEX_SHIFT) & 15);
        }

        volatile boolean shouldLogAverage = false;

        @Override
        public void coalesce(BlockingQueue<Coalescable> input, List<Coalescable> out,  int outSize) throws InterruptedException
        {
            if (input.drainTo(out, outSize) == 0)
            {
                out.add(input.take());
                input.drainTo(out, outSize - out.size());
            }

            for (Coalescable qm : out)
                logSample(qm.timestampNanos());

            if (DEBUG_COALESCING && shouldLogAverage)
            {
                shouldLogAverage = false;
                logger.info("MovingTimeHorizon average gap " + TimeUnit.NANOSECONDS.toMicros(averageGap()) + "μs");
            }

            int count = out.size();
            if (maybeSleep(count, averageGap(), maxCoalesceWindow, parker))
            {
                input.drainTo(out, outSize - out.size());
                int prevCount = count;
                count = out.size();
                for (int  i = prevCount; i < count; i++)
                    logSample(out.get(i).timestampNanos());
            }
        }
    }

    /*
     * Start coalescing by sleeping if the moving average is < the requested window.
     * The actual time spent waiting to coalesce will be the min( window, moving average * 2)
     * The actual amount of time spent waiting can be greater then the window. For instance
     * observed time spent coalescing was 400 microseconds with the window set to 200 in one benchmark.
     */
    @VisibleForTesting
    static class MovingAverageCoalescingStrategy extends CoalescingStrategy
    {
        private final int samples[] = new int[16];
        private long lastSample = 0;
        private int index = 0;
        private long sum = 0;

        private final long maxCoalesceWindow;

        public MovingAverageCoalescingStrategy(int maxCoalesceWindow, Parker parker, Logger logger, boolean debug)
        {
            super(parker, logger, debug);
            this.maxCoalesceWindow = TimeUnit.MICROSECONDS.toNanos(maxCoalesceWindow);
            for (int ii = 0; ii < samples.length; ii++)
                samples[ii] = Integer.MAX_VALUE;
            sum = Integer.MAX_VALUE * (long)samples.length;
            if (DEBUG_COALESCING) {
                new Thread() {
                    @Override
                    public void run() {
                        while (true) {
                            try
                            {
                                Thread.sleep(5000);
                            }
                            catch (InterruptedException e)
                            {
                                throw new AssertionError();
                            }
                            shouldLogAverage = true;
                        }
                    }
                }.start();
            }
        }

        private long logSample(int value)
        {
            assert(sumMatches());
            sum -= samples[index];
            sum += value;
            samples[index] = value;
            index++;
            index = index & ((1 << 4) - 1);
            assert(sumMatches());
            return sum / 16;
        }

        private long notifyOfSample(long sample)
        {
            if (sample > lastSample)
            {
                final int delta = (int)(Math.min(Integer.MAX_VALUE, sample - lastSample));
                lastSample = sample;
                return logSample(delta);
            }
            else
            {
                return logSample(1);
            }
        }

        volatile boolean shouldLogAverage = false;

        @Override
        public void coalesce(BlockingQueue<Coalescable> input, List<Coalescable> out,  int outSize) throws InterruptedException
        {
            if (input.drainTo(out, outSize) == 0)
            {
                out.add(input.take());
            }

            long average = notifyOfSample(out.get(0).timestampNanos());

            if (DEBUG_COALESCING && shouldLogAverage)
            {
                shouldLogAverage = false;
                logger.info("MovingAverage average gap " + TimeUnit.NANOSECONDS.toMicros(average) + "μs");
            }

            if (maybeSleep(out.size(), average, maxCoalesceWindow, parker))
            {
                input.drainTo(out, outSize - out.size());
                for (int ii = 1; ii < out.size(); ii++)
                    notifyOfSample(out.get(ii).timestampNanos());

                return;
            }

            input.drainTo(out, outSize - out.size());
            for (int ii = 1; ii < out.size(); ii++)
                notifyOfSample(out.get(ii).timestampNanos());
        }

        boolean sumMatches()
        {
            long recalc = 0;
            for (int sample : samples)
                recalc += sample;
            return recalc == sum;
        }
    }

    /*
     * A fixed strategy as a backup in case MovingAverage or TimeHorizongMovingAverage fails in some scenario
     */
    @VisibleForTesting
    static class FixedCoalescingStrategy extends CoalescingStrategy
    {
        private final long coalesceWindow;

        public FixedCoalescingStrategy(int coalesceWindowMicros, Parker parker, Logger logger, boolean debug)
        {
            super(parker, logger, debug);
            coalesceWindow = TimeUnit.MICROSECONDS.toNanos(coalesceWindowMicros);
        }

        @Override
        public void coalesce(BlockingQueue<Coalescable> input, List<Coalescable> out,  int outSize) throws InterruptedException
        {
            if (input.drainTo(out, outSize) == 0)
                out.add(input.take());
            else
                return;

            parker.park(coalesceWindow);

            input.drainTo(out, outSize - out.size());
        }
    }

    /*
     * A coalesscing strategy that just returns all currently available elements
     */
    @VisibleForTesting
    static class DisabledCoalescingStrategy extends CoalescingStrategy
    {

        public DisabledCoalescingStrategy(int coalesceWindowMicros, Parker parker, Logger logger, boolean debug)
        {
            super(parker, logger, debug);
        }

        @Override
        public void coalesce(BlockingQueue<Coalescable> input, List<Coalescable> out,  int outSize) throws InterruptedException
        {
            if (input.drainTo(out, outSize) == 0)
            {
                out.add(input.take());
                input.drainTo(out, outSize - out.size());
            }
        }
    }

    @VisibleForTesting
    static CoalescingStrategy newCoalescingStrategy(String strategy, int coalesceWindow, Parker parker, Logger logger, boolean debug)
    {
        String classname = null;
        String strategyCleaned = strategy.trim().toUpperCase();
        switch(strategyCleaned)
        {
        case "MOVINGAVERAGE":
            classname = MovingAverageCoalescingStrategy.class.getName();
            break;
        case "FIXED":
            classname = FixedCoalescingStrategy.class.getName();
            break;
        case "TIMEHORIZON":
            classname = TimeHorizonMovingAverageCoalescingStrategy.class.getName();
            break;
        case "DISABLED":
            classname = DisabledCoalescingStrategy.class.getName();
            break;
        default:
            classname = strategy;
        }

        try
        {
            Class<?> clazz = Class.forName(classname);

            if (!CoalescingStrategy.class.isAssignableFrom(clazz))
            {
                throw new RuntimeException(classname + " is not an instance of CoalescingStrategy");
            }

            Constructor<?> constructor = clazz.getConstructor(int.class, Parker.class, Logger.class, boolean.class);

            return (CoalescingStrategy)constructor.newInstance(coalesceWindow, parker, logger, debug);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public static CoalescingStrategy newCoalescingStrategy(String strategy, int coalesceWindow, Logger logger, boolean debug)
    {
        return newCoalescingStrategy(strategy, coalesceWindow, PARKER, logger, debug);
    }
}
