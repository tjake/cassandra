/**
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

package org.apache.cassandra.test.microbench;


import com.google.common.collect.Lists;
import com.yammer.metrics.Metrics;
import org.apache.cassandra.concurrent.*;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ExpiringMap;
import org.apache.cassandra.utils.FBUtilities;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@BenchmarkMode(Mode.All)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1,jvmArgsAppend = "-Xmx512M")
@Threads(32)
@State(Scope.Benchmark)
public class ThreadPoolImpls
{
    @Param({"4"})
    private int poolSize;

    private ExecutorService lbq;
    private ExecutorService shared;
    private ExecutorService disruptor1;
    private ExecutorService disruptor2;
    private ExecutorService forkjoin;


    @State(Scope.Thread)
    public static class ThreadState
    {
        @Param({"100"})
        int batchSize;

        Callable<Integer>[] c;
        Future<Integer>[] f;
    }

    @Setup
    public void setup() throws IOException, ConfigurationException
    {
        //Shut the statics down
        QueryProcessor.evictionCheckTimer.shutdownNow();
        StorageService.optionalTasks.shutdownNow();
        StorageService.tasks.shutdownNow();
        StorageService.scheduledTasks.shutdownNow();
        ExpiringMap.service.shutdownNow();
        StorageService.bgMonitor.reportThread.shutdownNow();
        Metrics.shutdown();


        lbq = new ThreadPoolExecutor(poolSize, poolSize, 60, TimeUnit.SECONDS, new LinkedTransferQueue<Runnable>(), new NamedThreadFactory("lbq"));


        disruptor1 = new DisruptorExecutorService(poolSize, 1024, false);

        disruptor2 = new DisruptorExecutorService2(poolSize, 1024, false);

        forkjoin = new ForkJoinPool(poolSize);


        SharedExecutorPool pool = new SharedExecutorPool("Foo");
        shared = new SEPExecutor(pool, poolSize, 1024);
        pool.executors.add((SEPExecutor)shared);
    }

   @TearDown
   public void teardown()
   {
        lbq.shutdown();
        shared.shutdown();
        disruptor1.shutdown();
        disruptor2.shutdown();
        forkjoin.shutdown();
   }

    //@Benchmark
    public void lbq(ThreadState state) throws ExecutionException, InterruptedException
    {
       run(state, lbq);
    }

    @Benchmark
    public void disruptor1(ThreadState state) throws ExecutionException, InterruptedException
    {
        run(state, disruptor1);
    }

    //@Benchmark
    public void disruptor2(ThreadState state) throws ExecutionException, InterruptedException
    {
        run(state, disruptor2);
    }

    @Benchmark
    public void shared(ThreadState state) throws ExecutionException, InterruptedException
    {
        run(state, shared);
    }

    //@Benchmark
    public void forkjoin(ThreadState state) throws ExecutionException, InterruptedException
    {
        run(state, forkjoin);
    }

    void run(ThreadState state, ExecutorService tpe) throws ExecutionException, InterruptedException
    {
        if (state.c == null)
        {
            shutdownAllBut(tpe);

            state.c = new Callable[state.batchSize];
            state.f = new Future[state.batchSize];

            for (int i = 0; i < state.batchSize; i++)
            {
                state.c[i] = new Callable<Integer>()
                {
                    @Override
                    public Integer call() throws Exception
                    {
                        return 1;
                    }
                };
            }
        }

        for (int i = 0; i < state.batchSize; i++)
            state.f[i] = tpe.submit(state.c[i]);

        for (int i = 0; i < state.batchSize; i++)
            state.f[i].get();
    }

    private synchronized void shutdownAllBut(ExecutorService tpe)
    {
        if (disruptor2 != tpe)
            disruptor2.shutdown();

        if (disruptor1 != tpe)
            disruptor1.shutdown();

        if (lbq != tpe)
            lbq.shutdown();

        if (shared != tpe)
            shared.shutdown();

        if (forkjoin != tpe)
            forkjoin.shutdown();
    }

}
