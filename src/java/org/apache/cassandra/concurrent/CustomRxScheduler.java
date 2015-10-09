package org.apache.cassandra.concurrent;

import com.google.common.util.concurrent.AbstractFuture;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.apache.cassandra.config.DatabaseDescriptor;
import rx.Scheduler;
import rx.Subscription;
import rx.functions.Action0;
import rx.internal.schedulers.ScheduledAction;
import rx.plugins.RxJavaPlugins;
import rx.plugins.RxJavaSchedulersHook;
import rx.subscriptions.Subscriptions;

import java.util.concurrent.*;

/**
 * RX scheduler based on our SEP executor
 */
public class CustomRxScheduler extends Scheduler
{
    public static final CustomRxScheduler compute = new CustomRxScheduler(DatabaseDescriptor.getNativeTransportMaxThreads(), 128, "worker", "compute");
    public static final CustomRxScheduler io = new CustomRxScheduler(DatabaseDescriptor.getConcurrentReaders(), Integer.MAX_VALUE, "worker", "io");

    final HashedWheelTimer wheelTimer = new HashedWheelTimer();
    final TracingAwareExecutorService executor;

    private CustomRxScheduler(int maxThreads, int maxQueued, String jmxPath, String name)
    {
        executor = SharedExecutorPool.SHARED.newExecutor(maxThreads, maxQueued, jmxPath, name);
    }

    @Override
    public Worker createWorker()
    {
        return new Worker();
    }

    class TimeoutFuture<T> extends AbstractFuture<T> implements TimerTask
    {
        private final Timeout timeout;
        private final Runnable action;

        TimeoutFuture(Runnable action, long delay, TimeUnit unit)
        {
            this.action = action;
            timeout = wheelTimer.newTimeout(this, delay, unit);
        }

        @Override
        protected boolean set(T value)
        {
            timeout.cancel();

            return true;
        }

        @Override
        protected boolean setException(Throwable throwable)
        {
            return super.setException(throwable);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning)
        {
            return timeout.cancel();
        }

        @Override
        public void run(Timeout timeout) throws Exception
        {
            action.run();
        }
    }



    class Worker extends Scheduler.Worker implements Subscription {
        private final RxJavaSchedulersHook schedulersHook;
        volatile boolean isUnsubscribed;

        /* package */
        public Worker() {
            schedulersHook = RxJavaPlugins.getInstance().getSchedulersHook();
        }

        @Override
        public Subscription schedule(final Action0 action) {
            return schedule(action, 0, null);
        }

        @Override
        public Subscription schedule(final Action0 action, long delayTime, TimeUnit unit) {
            if (isUnsubscribed) {
                return Subscriptions.empty();
            }
            return scheduleActual(action, delayTime, unit);
        }

        public ScheduledAction scheduleActual(final Action0 action, long delayTime, TimeUnit unit) {
            Action0 decoratedAction = schedulersHook.onSchedule(action);
            ScheduledAction run = new ScheduledAction(decoratedAction);
            Future<?> f;
            if (delayTime <= 0) {
                f = executor.submit(run);
            } else {
                f = new TimeoutFuture(run, delayTime, unit);
            }
            run.add(Subscriptions.from(f));

            return run;
        }

        @Override
        public void unsubscribe() {
            isUnsubscribed = true;
        }

        @Override
        public boolean isUnsubscribed() {
            return isUnsubscribed;
        }
    }


}