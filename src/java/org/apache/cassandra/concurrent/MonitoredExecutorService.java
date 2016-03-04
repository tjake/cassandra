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

package org.apache.cassandra.concurrent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.openhft.affinity.AffinityLock;
import net.openhft.affinity.AffinitySupport;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * Executor Service based on idea from Martin Thompson
 *
 * Added work stealing and thread affinity
 */
public class MonitoredExecutorService extends AbstractLocalAwareExecutorService
{
    private static final Logger logger = LoggerFactory.getLogger(MonitoredExecutorService.class);

    private static Thread monitorThread;
    private static final int globalMaxThreads = DatabaseDescriptor.getNativeTransportMaxThreads() +
                                                DatabaseDescriptor.getConcurrentReaders() +
                                                DatabaseDescriptor.getConcurrentWriters() +
                                                DatabaseDescriptor.getConcurrentCounterWriters() +
                                                FBUtilities.getAvailableProcessors();

    private static final List<MonitoredExecutorService> monitoredExecutiveServices = Lists.newCopyOnWriteArrayList();
    private static final ThreadFactory threadFactory = new NamedThreadFactory("SHARED-Work");

    private int lastSize = 0;

    private static ThreadWorker[] allWorkers;
    private static Thread[] allThreads;

    //Work queue per core
    private static Queue<FutureTask>[] localWorkQueues;
    private static Map<Thread, Queue<FutureTask>> threadIdLookup;

    public final Queue<FutureTask<?>> workQueue;
    private final String name;
    private final int maxThreads;
    private final int maxItems;

    public MonitoredExecutorService(String name, int maxThreads, int maxItems)
    {
        workQueue = new ConcurrentLinkedQueue<>();
        this.name = name;
        this.maxItems = maxItems;
        this.maxThreads = maxThreads;

        startMonitoring(this);
    }



    static Integer[] getReservableCPUs()
    {
        List<Integer> reservableCPUs = new ArrayList<>();

        for (int i = 0; i < AffinityLock.cpuLayout().cpus(); i++)
        {
            reservableCPUs.add(i);
        }

        assert !reservableCPUs.isEmpty();
        return reservableCPUs.toArray(new Integer[]{});
    }

    static class ThreadWorker implements Runnable
    {
        enum State
        {
            PARKED, WORKING
        }

        public volatile State state;
        public final int threadId;
        public final int cpuId;
        public final int coreId;
        public final int socketId;
        public final int localQueueOffset;

        private int[] workOrder;

        ThreadWorker(int threadId, int cpuId, int coreId, int socketId, int localQueueOffset)
        {
            this.threadId = threadId;
            this.cpuId = cpuId;
            this.coreId = coreId;
            this.socketId = socketId;
            this.localQueueOffset = localQueueOffset;
            this.state = State.WORKING;
        }

        public void park()
        {
            state = State.PARKED;
            LockSupport.park();
        }

        public void unpark()
        {
            state = State.WORKING;
            LockSupport.unpark(allThreads[threadId]);
        }

        @Override
        public void run()
        {
            try
            {
                logger.info("Assigning {} to cpu {} on core {} on socket {}", Thread.currentThread().getName(), cpuId, coreId, socketId);
                AffinitySupport.setAffinity(1L << cpuId);

                //Setup the work order for work stealing
                setWorkOrder();

                while (true)
                {
                    FutureTask<?> t;

                    //deal with spurious wakeups
                    if (state == State.PARKED)
                    {
                        park();
                    }
                    else
                    {
                        while ((t = findWork()) != null)
                        {
                            try
                            {
                                t.run();
                            } catch (Throwable ex)
                            {
                                JVMStabilityInspector.inspectThrowable(ex);
                                ex.printStackTrace();
                            }
                        }

                        //Nothing todo; park
                        park();
                    }
                }
            }
            finally
            {
                AffinitySupport.setAffinity(AffinityLock.BASE_AFFINITY);
            }
        }

        private void setWorkOrder()
        {
            if (workOrder != null)
                return;

            int index = 0;
            workOrder = new int[localWorkQueues.length];

            //Sort relative to self/cpu/core/socket distance
            //self
            workOrder[index++] = localQueueOffset;

            Set<Integer> seenOffsets = new HashSet<>();
            seenOffsets.add(localQueueOffset);

            //Same socket different core
            for (int tcpuId : getReservableCPUs())
            {
                int tcoreId = AffinityLock.cpuLayout().coreId(tcpuId);
                int tsocketId = AffinityLock.cpuLayout().socketId(tcpuId);
                int coresPerSocket = AffinityLock.cpuLayout().coresPerSocket();
                int offset = tcoreId + (tsocketId * coresPerSocket);

                if (offset == localQueueOffset || tsocketId != socketId)
                    continue;

                if (!seenOffsets.add(offset))
                    continue;

                workOrder[index++] = offset;
            }

            //Everything else...
            for (int offset = 0; offset < localWorkQueues.length; offset++)
            {
                if (!seenOffsets.add(offset))
                    continue;

                workOrder[index++] = offset;
            }

            logger.info("Work order for thread on {} {} {}", coreId, socketId, workOrder);
        }

        /**
         * Looks for work to steal from peers, if none found moves to main work queue
         * @return A task to work on
         */
        private FutureTask findWork()
        {
            FutureTask work = null;

            //Check local queues
            for (int i = 0, length = localWorkQueues.length; i < length; i++)
            {
                work = localWorkQueues[workOrder[i]].poll();

                if (work != null)
                    return work;
            }

            //Take from global queues
            for (int i = 0, length = monitoredExecutiveServices.size(); i < length; i++)
            {
                MonitoredExecutorService executor = monitoredExecutiveServices.get(i);

                work = executor.workQueue.poll();
                if (work != null)
                    return work;

            }

            return null;
        }
    }

    private void checkQueue()
    {
        final int size = 0; //queuedItems.get();
        int numUnparked = 0;
        int numRunning = 0;

        for (int i = 0; i < allWorkers.length; i++)
        {
            ThreadWorker t = allWorkers[i];
            if (t.state == ThreadWorker.State.WORKING) numRunning++;
        }

        if (size > 0 && (size >= lastSize || numRunning == 0))
        {
            for (int i = 0; i < allWorkers.length; i++) {
                ThreadWorker t = allWorkers[i];
                if (t.state == ThreadWorker.State.PARKED)
                {
                    t.unpark();
                    numUnparked++;
                    if (size == lastSize || numUnparked >= size) break;
                }
            }
        }

        lastSize = size;
    }

    private static synchronized void startMonitoring(MonitoredExecutorService service)
    {
        if (allWorkers == null)
        {
            Integer[] reservableCPUs = getReservableCPUs();

            allWorkers = new ThreadWorker[globalMaxThreads];
            allThreads = new Thread[globalMaxThreads];
            localWorkQueues = new Queue[AffinityLock.cpuLayout().coresPerSocket() * AffinityLock.cpuLayout().sockets()];
            threadIdLookup = new HashMap<>(globalMaxThreads);


            for (int i = 0; i < globalMaxThreads; i++)
            {
                //Round robin all cpus to evenly spread out threads across cores.
                int cpuId = reservableCPUs[ i % reservableCPUs.length ];
                int coreId = AffinityLock.cpuLayout().coreId(cpuId);
                int socketId = AffinityLock.cpuLayout().socketId(cpuId);
                int coresPerSocket = AffinityLock.cpuLayout().coresPerSocket();
                int localQueueOffset = coreId + (socketId * coresPerSocket);

                allWorkers[i] = new ThreadWorker(i, cpuId, coreId, socketId, localQueueOffset);

                if (localWorkQueues[localQueueOffset] == null)
                    localWorkQueues[localQueueOffset] = new ConcurrentLinkedQueue<>();

                allThreads[i] = threadFactory.newThread(allWorkers[i]);
                threadIdLookup.put(allThreads[i], localWorkQueues[localQueueOffset]);
            }

            for (int i = 0; i < globalMaxThreads; i++)
            {
                allThreads[i].setDaemon(true);
                allThreads[i].start();
            }
        }

        monitoredExecutiveServices.add(service);

        if (monitorThread != null)
            return;

        monitorThread = new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                while (true)
                {
                    for (int i = 0, length = monitoredExecutiveServices.size(); i < length; i++)
                    {
                        monitoredExecutiveServices.get(i).checkQueue();
                    }

                    LockSupport.parkNanos(1);
                }
            }
        });

        monitorThread.setName("monitor-executive-service-thread");
        monitorThread.setDaemon(true);
        monitorThread.start();
    }

    @Override
    protected void addTask(FutureTask<?> futureTask)
    {
        Queue<FutureTask> localQueue = threadIdLookup.get(Thread.currentThread());

        if (localQueue != null)
        {
            localQueue.add(futureTask);
        }
        else
        {
            workQueue.add(futureTask);
        }
    }

    protected void onCompletion()
    {

    }


    @Override
    public void maybeExecuteImmediately(Runnable command)
    {
        addTask(newTaskFor(command, null));
    }

    @Override
    public void shutdown()
    {

    }

    @Override
    public List<Runnable> shutdownNow()
    {
        return null;
    }

    @Override
    public boolean isShutdown()
    {
        return false;
    }

    @Override
    public boolean isTerminated()
    {
        return false;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
    {
        return false;
    }
}
