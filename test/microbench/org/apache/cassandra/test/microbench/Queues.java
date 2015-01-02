package org.apache.cassandra.test.microbench;

import org.jctools.queues.MpmcArrayQueue;
import org.jctools.queues.MpmcConcurrentQueueStateMarkers;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Control;

import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 20, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1,jvmArgsAppend = "-Xmx512M")
@State(Scope.Group)
public class Queues {
    private static final int CHAIN_LENGTH = 16; //Integer.getInteger("chain.length", 2);
    private static final int BURST_SIZE = Integer.getInteger("burst.size", 1);
    private static final Integer DUMMY_MESSAGE = 1;
    @SuppressWarnings("unchecked")
    private final static Queue<Integer>[] chain = new Queue[CHAIN_LENGTH];
    /**
     * This is a bit annoying, I need the threads to keep their queues, so each thread needs an index. The id
     * is used to pick the in/out queues.
     */
    private final static AtomicInteger idx = new AtomicInteger();
    private final static ThreadLocal<Integer> tlIndex = new ThreadLocal<Integer>() {
        protected Integer initialValue() {
            return idx.getAndIncrement();
        }
    };

    /**
     * Link in the chain passes events from chain[threadIndex] to chain[(id + 1) % CHAIN_LENGTH].
     * <p>
     * Note that while the state is per thread, the thread can change per iteration. We use the above thread
     * id to maintain the same queues are selected per thread.
     */
    @State(Scope.Thread)
    public static class Link {
        final Queue<Integer> in;
        final Queue<Integer> out;

        public Link() {
            int id = tlIndex.get();
            // the old in out, in out
            this.in = chain[id % CHAIN_LENGTH];
            this.out = chain[(id + 1) % CHAIN_LENGTH];
        }

        public void link() {
            // we could use the control here, but there's no reason as it is use externally and we only
            // really want to measure the ping method
            Integer e = in.poll();
            if (e != null) {
                out.offer(e);
            }
        }

        /**
         * We want to always start with an empty inbound. Iteration tear downs are synchronized.
         */
        @TearDown(Level.Iteration)
        public void clear() {
            // SPSC -> consumer must clear the queue.
            in.clear();
        }
    }

    /**
     * The source of events in the ring. Sends a burst of events into chain[(id + 1) % CHAIN_LENGTH] and waits
     * until the burst makes it through the ring back to chain[id].
     * <p>
     * Note that while the state is per thread, the thread can change per iteration. We use the above thread
     * id to maintain the same queues are selected per thread.
     */
    @State(Scope.Thread)
    public static class Source {
        final Queue<Integer> start;
        final Queue<Integer> end;

        public Source() {
            int id = tlIndex.get();
            // the source ties the knot in our ring
            this.end = chain[id % CHAIN_LENGTH];
            this.start = chain[(id + 1) % CHAIN_LENGTH];
        }

        public void ping(Control ctl) {
            for (int i = 0; i < BURST_SIZE; i++) {
                start.offer(DUMMY_MESSAGE);
            }
            for (int i = 0; i < BURST_SIZE; i++) {
                while (!ctl.stopMeasurement && end.poll() == null) {
                }
            }
        }

        /**
         * We want to always start with an empty inbound. Iteration tear downs are synchronized.
         */
        @TearDown(Level.Iteration)
        public void clear() {
            // SPSC -> consumer must clear the queue.
            end.clear();
        }
    }

    @Setup(Level.Trial)
    public void prepareChain() {
        // can't have group threads set to zero on a method, so can't handle the length of 1 case
        if (CHAIN_LENGTH < 2) {
            throw new IllegalArgumentException("Chain length must be 1 or more");
        }
        // This is an estimate, but for bounded queues if the burst size is more than actual ring capacity
        // the benchmark will hang/
        if (BURST_SIZE > 1024 * CHAIN_LENGTH >> 1) {
            throw new IllegalArgumentException("Batch size exceeds estimated capacity");
        }
        // initialize the chain
        for (int i = 0; i < CHAIN_LENGTH; i++) {
            chain[i] = new MpmcArrayQueue<>(1024);
            //chain[i] = new ConcurrentLinkedQueue<>();
        }
    }

    @Benchmark
    @Group("ring")
    @GroupThreads(1) // Must be one
    public void ping(Control ctl, Source s) {
        s.ping(ctl);
    }


    @Benchmark
    @Group("ring")
    @GroupThreads(15) // Must be CHAIN_LENGTH - 1
    public void loop(Link l) {
        l.link();
    }
}