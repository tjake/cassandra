package org.apache.cassandra.concurrent;

import rx.Scheduler;
import rx.Subscription;
import rx.functions.Action0;
import rx.internal.schedulers.ScheduledAction;
import rx.internal.util.RxRingBuffer;
import rx.plugins.RxJavaPlugins;
import rx.plugins.RxJavaSchedulersHook;
import rx.subscriptions.Subscriptions;

import java.util.concurrent.*;

/**
 * Created by jake on 10/30/14.
 */
public class CustomRxScheduler extends Scheduler
{
    TracingAwareExecutorService executor = new DisruptorExecutorService(Runtime.getRuntime().availableProcessors(), 1024, false);

    @Override
    public Worker createWorker()
    {
        return new Worker(executor);
    }

    static class Worker extends Scheduler.Worker implements Subscription {
        private final ExecutorService executor;
        private final RxJavaSchedulersHook schedulersHook;
        volatile boolean isUnsubscribed;

        /* package */
        public Worker(ExecutorService executor) {
            this.executor = executor;
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

        /**
         * @warn javadoc missing
         * @param action
         * @param delayTime
         * @param unit
         * @return
         */
        public ScheduledAction scheduleActual(final Action0 action, long delayTime, TimeUnit unit) {
            Action0 decoratedAction = schedulersHook.onSchedule(action);
            ScheduledAction run = new ScheduledAction(decoratedAction);
            Future<?> f;
            if (delayTime <= 0) {
                f = executor.submit(run);
            } else {
                throw new UnsupportedOperationException("Use a different scheduler");
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
