package org.apache.cassandra.concurrent;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Executor service that uses a Disruptor for message passing
 */
public class DisruptorExecutorService extends AbstractTracingAwareExecutorService
{
    public static class TaskEvent
    {
        private Runnable task;

        public void setTask(Runnable task)
        {
            this.task = task;
        }

        public Runnable getTask()
        {
            return task;
        }

        public void execute()
        {
            task.run();
        }

        public final static EventFactory<TaskEvent> factory = new EventFactory<TaskEvent>()
        {
            public TaskEvent newInstance()
            {
                return new TaskEvent();
            }
        };
    }

    /**
     * Convenience class for handling the batching semantics of consuming entries from a {@link RingBuffer}
     * and delegating the available events to an {@link EventHandler}.
     *
     * If the {@link EventHandler} also implements {@link LifecycleAware} it will be notified just after the thread
     * is started and just before the thread is shutdown.
     *
     */
    public final class TaskEventProcessor
            implements EventProcessor
    {
        private final AtomicBoolean running = new AtomicBoolean(false);
        private ExceptionHandler exceptionHandler = new FatalExceptionHandler();
        private final DataProvider<TaskEvent> dataProvider;
        private final SequenceBarrier sequenceBarrier;
        private final Sequence sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
        private final int ordinal;
        private final int totalProcessors;

        /**
         * Construct a {@link EventProcessor} that will automatically track the progress by updating its sequence when
         * the {@link EventHandler#onEvent(Object, long, boolean)} method returns.
         *
         * @param dataProvider to which events are published.
         * @param sequenceBarrier on which it is waiting.
         */
        public TaskEventProcessor(final DataProvider<TaskEvent> dataProvider,
                                  final SequenceBarrier sequenceBarrier,
                                  final int ordinal,
                                  final int totalProcessors)
        {
            this.dataProvider = dataProvider;
            this.sequenceBarrier = sequenceBarrier;
            this.ordinal = ordinal;
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
        }


        /**
         * Set a new {@link ExceptionHandler} for handling exceptions propagated out of the {@link BatchEventProcessor}
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

            TaskEvent event = null;
            long nextSequence = sequence.get() + 1 + ordinal; //Start at processor offset in barrier
            try
            {
                while (true)
                {
                    try
                    {
                        long availableSequence = sequenceBarrier.waitFor(nextSequence);
                        for (; nextSequence <= availableSequence; nextSequence += totalProcessors )
                        {
                            event = dataProvider.get(nextSequence);
                            event.execute();
                        }

                        sequence.set(nextSequence - 1);
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
                        exceptionHandler.handleEventException(ex, nextSequence, event);
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

    public DisruptorExecutorService(final int workerPoolSize, int disruptorRingSize, boolean isSingleProducer)
    {
        executor = Executors.newFixedThreadPool(workerPoolSize, new NamedThreadFactory("disruptor1"));

        disruptor = new Disruptor<>(TaskEvent.factory, disruptorRingSize, executor, isSingleProducer ? ProducerType.SINGLE : ProducerType.MULTI, new BlockingWaitStrategy());

        ringBuffer = disruptor.getRingBuffer();

        TaskEventProcessor processors[] = new TaskEventProcessor[workerPoolSize];
        for (int i = 0; i < workerPoolSize; i++)
        {
            final int ordinal = i;
            processors[i] = new TaskEventProcessor(ringBuffer, ringBuffer.newBarrier(), ordinal, workerPoolSize);
        }

        disruptor.handleEventsWith(processors);
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

        if(isShutdown())
            return;

        disruptor.halt();

        executor.shutdownNow();

    }

    @Override
    public List<Runnable> shutdownNow()
    {

        executor.shutdownNow();

        disruptor.halt();


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