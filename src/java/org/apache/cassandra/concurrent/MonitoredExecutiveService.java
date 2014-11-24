package org.apache.cassandra.concurrent;

import com.google.common.collect.Lists;
import org.apache.cassandra.utils.JVMStabilityInspector;

import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Executor Service based on idea from Martin Thompson
 */
public class MonitoredExecutiveService extends AbstractTracingAwareExecutorService
{
    private static Thread monitorThread;
    private static List<MonitoredExecutiveService> monitoredExecutiveServices = Lists.newCopyOnWriteArrayList();

    private final Thread[] allThreads;
    private final Thread[] parkedThreads;
    private final SpmcArrayQueue<FutureTask<?>> workQueue;

    private MonitoredExecutiveService(int maxThreads, int maxItems, ThreadFactory threadFactory)
    {
        allThreads = new Thread[maxThreads];
        parkedThreads = new Thread[maxThreads];
        workQueue = new SpmcArrayQueue<>(maxItems);

        for (int i = 0; i < maxThreads; i++)
        {
            final int threadId = i;
            allThreads[i] = threadFactory.newThread(new Runnable()
            {
                @Override
                public void run()
                {
                    while (true)
                    {

                        FutureTask<?> t;
                        while ((t = workQueue.poll()) != null)
                        {
                            try
                            {
                                t.run();
                            }
                            catch (Throwable ex)
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
            });

            allThreads[i].start();
        }

        startMonitoring(this);
    }

    private void check()
    {

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
        workQueue.add(futureTask);
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
