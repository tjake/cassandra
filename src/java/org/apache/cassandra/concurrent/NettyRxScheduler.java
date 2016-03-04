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

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.GlobalEventExecutor;
import rx.Scheduler;
import rx.Subscription;
import rx.functions.Action0;
import rx.internal.schedulers.ScheduledAction;
import rx.internal.util.SubscriptionList;
import rx.subscriptions.CompositeSubscription;
import rx.subscriptions.Subscriptions;

/**
 *
 */
public class NettyRxScheduler extends Scheduler
{
    public final static FastThreadLocal<NettyRxScheduler> localNettyEventLoop = new FastThreadLocal<NettyRxScheduler>()
    {
        protected NettyRxScheduler initialValue()
        {
            return new NettyRxScheduler(GlobalEventExecutor.INSTANCE);
        }
    };

    final EventExecutor eventLoop;

    public static NettyRxScheduler instance()
    {
        return localNettyEventLoop.get();
    }

    public static NettyRxScheduler instance(EventExecutor loop)
    {
        NettyRxScheduler scheduler = localNettyEventLoop.get();
        if (scheduler == null || scheduler.eventLoop != loop)
        {
            scheduler = new NettyRxScheduler(loop);
            localNettyEventLoop.set(scheduler);
        }

        return scheduler;
    }

    private NettyRxScheduler(EventExecutor eventLoop)
    {
        this.eventLoop = eventLoop;
        localNettyEventLoop.set(this);
    }

    @Override
    public Worker createWorker()
    {
        return new Worker(eventLoop.next());
    }

    public static class Worker extends Scheduler.Worker
    {
        private final EventExecutor nettyEventLoop;

        private final SubscriptionList serial;
        private final CompositeSubscription timed;
        private final SubscriptionList both;

        volatile boolean isUnsubscribed;

        public Worker(EventExecutor nettyEventLoop)
        {
            this.nettyEventLoop = nettyEventLoop;

            serial = new SubscriptionList();
            timed = new CompositeSubscription();
            both = new SubscriptionList(serial, timed);
        }

        @Override
        public Subscription schedule(final Action0 action) {
            return schedule(action, 0, TimeUnit.HOURS);
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

            ScheduledAction run;
            if (delayTime <= 0) {
                run = new ScheduledAction(action, serial);
                serial.add(run);
                nettyEventLoop.next().submit(run);
            } else
            {
                run = new ScheduledAction(action, timed);
                timed.add(run);

                final Future<?> result = nettyEventLoop.next().schedule(run, delayTime, unit);
                Subscription cancelFuture = Subscriptions.create(() -> result.cancel(false));

                run.add(cancelFuture); /*An unsubscribe of the returned sub should cancel the future*/
            }

            return run;
        }

        @Override
        public void unsubscribe() {
            both.unsubscribe();
        }

        @Override
        public boolean isUnsubscribed() {
            return both.isUnsubscribed();
        }
    }
}
