package org.apache.cassandra.concurrent;

import com.google.common.util.concurrent.Uninterruptibles;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.cassandra.concurrent.DisruptorExecutorService.TaskEvent;

/**
 * Executor service that uses a Disruptor for message passing
 */
public class DisruptorExecutorService2 extends AbstractTracingAwareExecutorService
{
    /**
     * Convenience class for handling the batching semantics of consuming entries from a {@link com.lmax.disruptor.RingBuffer}
     * and delegating the available events to an {@link com.lmax.disruptor.EventHandler}.
     *
     * If the {@link com.lmax.disruptor.EventHandler} also implements {@link com.lmax.disruptor.LifecycleAware} it will be notified just after the thread
     * is started and just before the thread is shutdown.
     *
     */
    public final class TaskEventProcessor
            implements EventProcessor
    {
        private final AtomicBoolean running = new AtomicBoolean(false);
        private ExceptionHandler exceptionHandler = new FatalExceptionHandler();
        private final RingBuffer<TaskEvent> ringBuffer;
        private final SequenceBarrier sequenceBarrier;
        private final Sequence sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
        private final int totalProcessors;
        private Disruptor<TaskEvent> disruptors[];
        private ExecutorService executors[];

        /**
         * Construct a {@link com.lmax.disruptor.EventProcessor} that will automatically track the progress by updating its sequence when
         * the {@link com.lmax.disruptor.EventHandler#onEvent(Object, long, boolean)} method returns.
         *
         * @param ringBuffer to which events are published.
         * @param sequenceBarrier on which it is waiting.
         */
        public TaskEventProcessor(final RingBuffer<TaskEvent> ringBuffer,
                                  final SequenceBarrier sequenceBarrier,
                                  final int totalProcessors)
        {
            this.ringBuffer = ringBuffer;
            this.sequenceBarrier = sequenceBarrier;



            //The theory is all threads are notified from the ringbuffer
            //So it's better to have a to have a singe thread per pool
            //dispatch to other threads with single ring buffer
            disruptors = new Disruptor[totalProcessors];
            executors = new ExecutorService[totalProcessors];
            for (int i = 0; i < totalProcessors; i++)
            {
                executors[i] = Executors.newSingleThreadExecutor();
                disruptors[i] = new Disruptor<>(TaskEvent.factory, ringBuffer.getBufferSize(), executors[i] , ProducerType.SINGLE, new YieldingWaitStrategy());
                disruptors[i].handleEventsWith(new EventHandler<TaskEvent>()
                {
                    @Override
                    public void onEvent(TaskEvent taskEvent, long l, boolean b) throws Exception
                    {
                        taskEvent.execute();
                    }
                });

                disruptors[i].start();
            }

            this.totalProcessors = totalProcessors;
        }

        @Override
        public Sequence getSequence()
        {
            return sequence;
        }

        @Override
        public void halt()
        {
            running.set(false);
            sequenceBarrier.alert();
            executor.shutdown();

            for ( int i = 0; i < totalProcessors; i++)
            {
                disruptors[i].halt();
                executors[i].shutdown();
            }

        }


        /**
         * Set a new {@link com.lmax.disruptor.ExceptionHandler} for handling exceptions propagated out of the {@link com.lmax.disruptor.BatchEventProcessor}
         *
         * @param exceptionHandler to replace the existing exceptionHandler.
         */
        public void setExceptionHandler(final ExceptionHandler exceptionHandler)
        {
            if (null == exceptionHandler)
            {
                throw new NullPointerException();
            }

            this.exceptionHandler = exceptionHandler;
        }

        /**
         * It is ok to have another thread rerun this method after a halt().
         *
         * @throws IllegalStateException if this object instance is already running in a thread
         */
        @Override
        public void run()
        {
            if (!running.compareAndSet(false, true))
            {
                throw new IllegalStateException("Thread is already running");
            }
            sequenceBarrier.clearAlert();

            notifyStart();

            long nextSequence = sequence.get() + 1;
            try
            {
                while (true)
                {
                    try
                    {
                        long availableSequence = sequenceBarrier.waitFor(nextSequence);
                        while (nextSequence <= availableSequence)
                        {
                            final TaskEvent event = ringBuffer.get(nextSequence);
                            disruptors[(int)(nextSequence % totalProcessors)].getRingBuffer().publishEvent(new EventTranslator<TaskEvent>()
                            {
                                @Override
                                public void translateTo(TaskEvent taskEvent, long l)
                                {
                                    taskEvent.setTask(event.getTask());
                                }
                            });

                            nextSequence++;
                        }

                        sequence.set(availableSequence);
                    }
                    catch (final TimeoutException e)
                    {
                        notifyTimeout(sequence.get());
                    }
                    catch (final AlertException ex)
                    {
                        if (!running.get())
                        {
                            break;
                        }
                    }
                    catch (final Throwable ex)
                    {
                        exceptionHandler.handleEventException(ex, nextSequence, null);
                        sequence.set(nextSequence);
                        nextSequence++;
                    }
                }
            }
            finally
            {
                notifyShutdown();
                running.set(false);
            }
        }

        private void notifyTimeout(final long availableSequence)
        {

        }

        /**
         * Notifies the EventHandler when this processor is starting up
         */
        private void notifyStart()
        {
        }

        /**
         * Notifies the EventHandler immediately prior to this processor shutting down
         */
        private void notifyShutdown()
        {

        }
    }

    final ExecutorService executor;
    final Disruptor<TaskEvent> disruptor;
    final RingBuffer ringBuffer;

    public DisruptorExecutorService2(final int workerPoolSize, int disruptorRingSize, boolean isSingleProducer)
    {
        executor = Executors.newFixedThreadPool(workerPoolSize, new NamedThreadFactory("disruptor-2"));

        disruptor = new Disruptor<>(TaskEvent.factory, disruptorRingSize, executor, isSingleProducer ? ProducerType.SINGLE : ProducerType.MULTI, new YieldingWaitStrategy());

        ringBuffer = disruptor.getRingBuffer();

        TaskEventProcessor processor =  new TaskEventProcessor(ringBuffer, ringBuffer.newBarrier(), workerPoolSize);

        disruptor.handleEventsWith(processor);
        disruptor.start();
    }

    @Override
    protected void addTask(final FutureTask<?> futureTask)
    {
        EventTranslator translator = new EventTranslator<TaskEvent>()
        {
            @Override
            public void translateTo(TaskEvent o, long sequence)
            {
                o.setTask(futureTask);
            }
        };

        ringBuffer.publishEvent(translator);
    }

    @Override
    protected void onCompletion()
    {

    }

    @Override
    public void maybeExecuteImmediately(final Runnable command)
    {

        /*command.run();
        return;
*/



        EventTranslator translator = new EventTranslator<TaskEvent>()
        {
            @Override
            public void translateTo(TaskEvent o, long sequence)
            {
                o.setTask(command);
            }
        };

        ringBuffer.publishEvent(translator);

    }

    @Override
    public void shutdown()
    {
        if (isShutdown())
            return;

        disruptor.halt();

        executor.shutdown();

    }

    @Override
    public List<Runnable> shutdownNow()
    {

        disruptor.halt();

        executor.shutdownNow();

        return null;
    }

    @Override
    public boolean isShutdown()
    {
        return executor.isShutdown();
    }

    @Override
    public boolean isTerminated()
    {
        return false;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
    {
        disruptor.shutdown();
        return true;
    }
}
