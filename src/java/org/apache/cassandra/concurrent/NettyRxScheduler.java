package org.apache.cassandra.concurrent;

import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import rx.Scheduler;
import rx.Subscription;
import rx.functions.Action;
import rx.functions.Action0;
import rx.internal.schedulers.ScheduledAction;
import rx.plugins.RxJavaPlugins;
import rx.plugins.RxJavaSchedulersHook;
import rx.subscriptions.Subscriptions;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Created by jake on 10/31/14.
 */
public class NettyRxScheduler extends Scheduler
{

    public static ThreadLocal<EventLoop> localNettyEventLoop = new ThreadLocal<>();

    final EventLoop eventLoop;

    public NettyRxScheduler(EventLoop eventLoop)
    {
        this.eventLoop = eventLoop;
        localNettyEventLoop.set(eventLoop);
    }


    @Override
    public Worker createWorker()
    {
        return new Worker(eventLoop);
    }

    public static class Worker extends Scheduler.Worker implements Subscription
    {
        private final RxJavaSchedulersHook schedulersHook;
        private final EventLoop nettyEventLoop;
        volatile boolean isUnsubscribed;

        public Worker(EventLoop nettyEventLoop)
        {
            this.nettyEventLoop = nettyEventLoop;
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

            if (action instanceof NettyAction0)
                ((NettyAction0)action).setNettyEventLoop(nettyEventLoop);

            Action0 decoratedAction = schedulersHook.onSchedule(action);
            ScheduledAction run = new ScheduledAction(decoratedAction);
            Future<?> f;
            if (delayTime <= 0) {
                f = nettyEventLoop.submit(run);
            } else {
                f = nettyEventLoop.schedule(run, delayTime, unit);
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

    public static interface NettyAction0 extends Action
    {
        public void setNettyEventLoop(EventLoop eventLoop);
    }

}
