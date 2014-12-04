package org.apache.cassandra.concurrent;

import com.google.common.collect.Lists;
import net.openhft.affinity.*;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * Executor Service based on idea from Martin Thompson
 *
 * Added work stealing and thread affinity
 */
public class MonitoredExecutiveService extends AbstractTracingAwareExecutorService
{
    private static final Logger logger = LoggerFactory.getLogger(MonitoredExecutiveService.class);

    private static Thread monitorThread;
    private static final int globalMaxThreads = DatabaseDescriptor.getNativeTransportMaxThreads() +
                                        DatabaseDescriptor.getConcurrentReaders() +
                                        DatabaseDescriptor.getConcurrentWriters() +
                                        DatabaseDescriptor.getConcurrentCounterWriters() +
                                        FBUtilities.getAvailableProcessors();

    private static final List<MonitoredExecutiveService> monitoredExecutiveServices = Lists.newCopyOnWriteArrayList();
    private static final ThreadFactory threadFactory = new NamedThreadFactory("SHARED-Work");

    private int lastSize = 0;

    private static ThreadWorker[] allWorkers;
    private static Thread[] allThreads;

    public final Queue<FutureTask<?>> workQueue;
    private final String name;
    private final int maxThreads;
    private final int maxItems;
    private final AtomicInteger queuedItems = new AtomicInteger(0);
    private final AtomicInteger activeItems = new AtomicInteger(0);

    public MonitoredExecutiveService(String name, int maxThreads, int maxItems)
    {
        super(name);

        workQueue = new ConcurrentLinkedQueue<>();
        this.name = name;
        this.maxItems = maxItems;
        this.maxThreads = maxThreads;

        startMonitoring(this);
    }

    void returnPermit()
    {
        activeItems.decrementAndGet();
    }

    boolean takePermit()
    {
        if (activeItems.incrementAndGet() >= maxThreads)
        {
            activeItems.decrementAndGet();
            return false;
        }

        return true;
    }

    static class ThreadWorker implements Runnable
    {
        enum State
        {
            PARKED, WORKING
        }

        public volatile State state;
        public final int threadId;

        ThreadWorker(int threadId)
        {
            this.threadId = threadId;
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
                logger.info("Closed worker thread");
            }
        }



        /**
         * Looks for work to steal from peers, if none found moves to main work queue
         * @return A task to work on
         */
        private FutureTask findWork()
        {
            FutureTask work = null;


            //Take from global queues
            for (int i = 0, length = monitoredExecutiveServices.size(); i < length; i++)
            {
                MonitoredExecutiveService executor = monitoredExecutiveServices.get((threadId + i) % length );

                if (executor.takePermit())
                {
                    work = executor.workQueue.poll();
                    if (work == null)
                    {
                        executor.returnPermit();
                    }
                    else
                    {
                        return work;
                    }
                }
            }

            return null;
        }
    }

    private void checkQueue()
    {
        //Avoid checking if we are over the specified limit for this executor
        if (activeItems.get() >= maxThreads)
            return;

        final int size = queuedItems.get();
        final int halfSize = size / 2;
        int numUnparked = 0;
        int numRunning = 0;

        for (int i = 0; i < allWorkers.length; i++)
        {
            ThreadWorker t = allWorkers[i];
            if (t.state == ThreadWorker.State.WORKING)
            {
                numRunning++;
                break;
            }
        }

        if (size > 0 && (size >= lastSize || numRunning == 0))
        {
            for (int i = 0; i < allWorkers.length; i++) {
                ThreadWorker t = allWorkers[i];
                if (t.state == ThreadWorker.State.PARKED)
                {
                    t.unpark();
                    numUnparked++;
                    if (numUnparked >= halfSize) break;
                }
            }
        }

        lastSize = size;
    }

    private static synchronized void startMonitoring(MonitoredExecutiveService service)
    {
        if (allWorkers == null)
        {
            allWorkers = new ThreadWorker[globalMaxThreads];
            allThreads = new Thread[globalMaxThreads];

            for (int i = 0; i < globalMaxThreads; i++)
            {
                allWorkers[i] = new ThreadWorker(i);
                allThreads[i] = threadFactory.newThread(allWorkers[i]);
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
        int queueLength = queuedItems.incrementAndGet();

        if (queueLength <= maxItems)
        {
            workQueue.add(futureTask);
        }
        else
        {
            queuedItems.decrementAndGet();
            throw new RuntimeException("Queue is full");
        }
    }

    @Override
    protected void onCompletion()
    {
        queuedItems.decrementAndGet();
        returnPermit();
    }

    @Override
    public void maybeExecuteImmediately(Runnable command)
    {
        if (takePermit())
        {
            newTaskFor(command, null).run();
        }
        else
        {
            addTask(newTaskFor(command, null));
        }
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
