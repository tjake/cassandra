package org.apache.cassandra.concurrent;

import com.google.common.collect.Lists;
import org.apache.cassandra.utils.JVMStabilityInspector;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * Executor Service based on idea from Martin Thompson
 */
public class MonitoredExecutiveService extends AbstractTracingAwareExecutorService
{
    private static Thread monitorThread;
    private static List<MonitoredExecutiveService> monitoredExecutiveServices = Lists.newCopyOnWriteArrayList();

    //public static final MonitoredExecutiveService shared = new MonitoredExecutiveService("Shared-Worker", 128, 8192, new NamedThreadFactory("SHARED-Work"));

    private int lastSize = 0;
    private final Thread[] allThreads;
    private final Thread[] parkedThreads;
    private final Deque<FutureTask>[] localWorkQueue;
    private final Deque<FutureTask<?>> workQueue;
    private final Map<Thread, Deque<FutureTask>> threadIdLookup;
    private final int maxItems;
    private final AtomicInteger currentItems = new AtomicInteger(0);

    public MonitoredExecutiveService(String name, int maxThreads, int maxItems, ThreadFactory threadFactory)
    {
        super(name);

        allThreads = new Thread[maxThreads];
        parkedThreads = new Thread[maxThreads];
        localWorkQueue = new Deque[maxThreads];
        threadIdLookup = new HashMap<>(maxThreads);

        workQueue = new ConcurrentLinkedDeque<>();
        this.maxItems = maxItems;

        for (int i = 0; i < maxThreads; i++)
        {
            final int threadId = i;

            localWorkQueue[i] = new ConcurrentLinkedDeque<>();

            allThreads[i] = threadFactory.newThread(new Runnable()
            {
                @Override
                public void run()
                {
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

                private FutureTask findWork()
                {
                    FutureTask work;

                    //Check local queue first
                    work = localWorkQueue[threadId].pollFirst();
                    if (work != null)
                        return work;

                    for (int i = 0; i < localWorkQueue.length; i++)
                    {
                        if (i == threadId) continue;

                        work = localWorkQueue[i].pollFirst();

                        if (work != null)
                            return work;
                    }

                    return workQueue.poll();
                }
            });

            threadIdLookup.put(allThreads[i], localWorkQueue[i]);

            allThreads[i].start();
        }

        startMonitoring(this);
    }



    private void check()
    {
        final int size = currentItems.get();
        int numUnparked = 0;
        int numRunning = 0;

        for (int i = 0; i < parkedThreads.length; i++) {
            Thread t = parkedThreads[i];
            if (t == null) numRunning++;
        }

        if (size >= lastSize || numRunning == 0)
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
            Deque<FutureTask> localQueue = threadIdLookup.get(Thread.currentThread());

            if (localQueue != null)
            {
                localQueue.offerFirst(futureTask);
            } else
            {
                if (ThreadLocalRandom.current().nextBoolean())
                    workQueue.addFirst(futureTask);
                else
                    workQueue.addLast(futureTask);
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
