package org.apache.cassandra.concurrent;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import net.openhft.affinity.*;
import net.openhft.affinity.impl.NoCpuLayout;
import net.openhft.affinity.impl.VanillaCpuLayout;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.LockSupport;

/**
 * Executor Service based on idea from Martin Thompson
 */
public class MonitoredExecutiveService extends AbstractTracingAwareExecutorService
{
    private static final Logger logger = LoggerFactory.getLogger(MonitoredExecutiveService.class);

    private static Thread monitorThread;
    private static List<MonitoredExecutiveService> monitoredExecutiveServices = Lists.newCopyOnWriteArrayList();

    //Lazy Singleton
    public static final Supplier<MonitoredExecutiveService> shared = Suppliers.memoize(new Supplier<MonitoredExecutiveService>()
    {
        @Override
        public MonitoredExecutiveService get()
        {
            MonitoredExecutiveService svc =  new MonitoredExecutiveService("Shared-Worker",
                    DatabaseDescriptor.getNativeTransportMaxThreads() +
                            DatabaseDescriptor.getConcurrentReaders() +
                            DatabaseDescriptor.getConcurrentWriters() +
                            DatabaseDescriptor.getConcurrentCounterWriters() +
                            FBUtilities.getAvailableProcessors()
                    , 8192, new NamedThreadFactory("SHARED-Work"));

            svc.start();

            return svc;
        }
    });

    private boolean started;
    private int lastSize = 0;
    private final ThreadWorker[] allWorkers;
    private final Thread[] allThreads;
    private final Thread[] parkedThreads;
    private final Queue<FutureTask>[] localWorkQueues;
    private final Queue<FutureTask<?>> workQueue;
    private final Map<Thread, Queue<FutureTask>> threadIdLookup;
    private final int maxItems;
    private final AtomicInteger currentItems = new AtomicInteger(0);

    public MonitoredExecutiveService(String name, int maxThreads, int maxItems, ThreadFactory threadFactory)
    {
        super(name);

        started = false;
        allWorkers = new ThreadWorker[maxThreads];
        allThreads = new Thread[maxThreads];
        parkedThreads = new Thread[maxThreads];
        localWorkQueues = new Queue[maxThreads];
        threadIdLookup = new HashMap<>(maxThreads);

        workQueue = new ConcurrentLinkedQueue<>();
        this.maxItems = maxItems;

        Integer[] reservableCPUs = getReservableCPUs();

        for (int i = 0; i < maxThreads; i++)
        {
            //Round robin all cpus to evenly spread out threads across cores.
            int cpuId = reservableCPUs[ i % reservableCPUs.length ];

            localWorkQueues[i] = new ConcurrentLinkedQueue<>();
            allWorkers[i] = new ThreadWorker(i, cpuId);
            allThreads[i] = threadFactory.newThread(allWorkers[i]);
            threadIdLookup.put(allThreads[i], localWorkQueues[i]);
        }
    }

    public synchronized void start()
    {
        if (started)
            return;

        for (int i = 0; i < allThreads.length; i++)
        {
            allThreads[i].start();
        }

        startMonitoring(this);
    }


    static Integer[] getReservableCPUs()
    {
        List<Integer> reservableCPUs = new ArrayList<>();

        for (int i = 0; i < AffinityLock.cpuLayout().cpus(); i++)
        {
            boolean reservable = ((AffinityLock.RESERVED_AFFINITY >> i) & 1) != 0;
            //if (!reservable)
            //    continue;

            reservableCPUs.add(i);
        }

        assert !reservableCPUs.isEmpty();
        return reservableCPUs.toArray(new Integer[]{});
    }

    class ThreadWorker implements Runnable
    {
        final int threadId;
        final int cpuId;
        final int coreId;
        final int socketId;
        private int[] workOrder;

        ThreadWorker(int threadId, int cpuId)
        {
            this.threadId = threadId;
            this.cpuId = cpuId;
            this.coreId = AffinityLock.cpuLayout().coreId(cpuId);
            this.socketId = AffinityLock.cpuLayout().socketId(cpuId);
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
                    while ((t = findWork()) != null)
                    {
                        try
                        {
                            currentItems.decrementAndGet();
                            t.run();
                        } catch (Throwable ex)
                        {
                            JVMStabilityInspector.inspectThrowable(ex);
                            ex.printStackTrace();
                        }
                    }

                    //Nothing todo; park
                    parkedThreads[threadId] = allThreads[threadId];
                    LockSupport.park();
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
            workOrder = new int[allThreads.length];

            //Sort relative to self/cpu/core/socket distance

            //self
            workOrder[index++] = threadId;

            //cpu
            for (int i = 0; i < allThreads.length; i++)
            {
                if (allWorkers[i].threadId == threadId)
                    continue;

                if (allWorkers[i].cpuId == cpuId)
                    workOrder[index++] = allWorkers[i].threadId;
            }

            //core
            for (int i = 0; i < allThreads.length; i++)
            {
                if (allWorkers[i].threadId == threadId)
                    continue;

                if (allWorkers[i].cpuId == cpuId)
                    continue;

                if (allWorkers[i].coreId == coreId)
                    workOrder[index++] = allWorkers[i].threadId;
            }

            //socket
            for (int i = 0; i < allThreads.length; i++)
            {
                if (allWorkers[i].threadId == threadId)
                    continue;

                if (allWorkers[i].cpuId == cpuId)
                    continue;

                if (allWorkers[i].coreId == coreId)
                    continue;

                if (allWorkers[i].socketId == socketId)
                    workOrder[index++] = allWorkers[i].threadId;
            }

            //Anything else
            for (int i = 0; i < allThreads.length; i++)
            {
                if (allWorkers[i].threadId == threadId)
                    continue;

                if (allWorkers[i].cpuId == cpuId)
                    continue;

                if (allWorkers[i].coreId == coreId)
                    continue;

                if (allWorkers[i].socketId == socketId)
                    continue;

                workOrder[index++] = allWorkers[i].threadId;
            }
        }

        /**
         * Looks for work to steal from peers, if none found moves to main work queue
         * @return A task to work on
         */
        private FutureTask findWork()
        {
            FutureTask work = null;

            for (int i = 0, length = localWorkQueues.length; i < length; i++)
            {
                work = localWorkQueues[workOrder[i]].poll();

                if (work != null)
                    return work;
            }

            //Take from global pool
            return workQueue.poll();
        }
    }

    /**
     *
     */
    private void check()
    {
        final int size = currentItems.get();
        int numUnparked = 0;
        int numRunning = 0;

        for (int i = 0; i < parkedThreads.length; i++) {
            Thread t = parkedThreads[i];
            if (t == null) numRunning++;
        }

        if (size > 0 && (size >= lastSize || numRunning == 0))
        {
            for (int i = 0; i < parkedThreads.length; i++) {
                Thread t = parkedThreads[i];
                if (t != null)
                {
                    parkedThreads[i] = null;
                    LockSupport.unpark(t);

                    numUnparked++;
                    if (size == lastSize || numUnparked >= size) break;
                }
            }
        }

        lastSize = size;
    }

    private static synchronized void startMonitoring(MonitoredExecutiveService service)
    {

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
                        monitoredExecutiveServices.get(i).check();
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
        int queueLength = currentItems.incrementAndGet();

        if (queueLength <= maxItems)
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
        else
        {
            currentItems.decrementAndGet();
            throw new RuntimeException("Queue is full");
        }
    }

    @Override
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
