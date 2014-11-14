package org.apache.cassandra.concurrent;

import rx.Subscription;
import rx.functions.Action0;
import rx.subscriptions.CompositeSubscription;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * Created by jake on 11/13/14.
 */
public class NewScheduledAction implements Runnable, Subscription
{
    final NewCompositeSubscription cancel;
    final Action0 action;
    volatile int once;
    static final AtomicIntegerFieldUpdater<NewScheduledAction> ONCE_UPDATER
            = AtomicIntegerFieldUpdater.newUpdater(NewScheduledAction.class, "once");

    public NewScheduledAction(Action0 action) {
        this.action = action;
        this.cancel = new NewCompositeSubscription();
    }

    @Override
    public void run() {
        try {
            action.call();
        } finally {
            unsubscribe();
        }
    }

    @Override
    public boolean isUnsubscribed() {
        return cancel.isUnsubscribed();
    }

    @Override
    public void unsubscribe() {
        if (ONCE_UPDATER.compareAndSet(this, 0, 1)) {
            cancel.unsubscribe();
        }
    }

    /**
     * @warn javadoc missing
     *
     * @param s
     * @warn param "s" undescribed
     */
    public void add(Subscription s) {
        cancel.add(s);
    }

    /**
     * Adds a parent {@link CompositeSubscription} to this {@code ScheduledAction} so when the action is
     * cancelled or terminates, it can remove itself from this parent.
     *
     * @param parent
     *            the parent {@code CompositeSubscription} to add
     */
    public void addParent(CompositeSubscription parent) {
        cancel.add(new Remover(this, parent));
    }

    /** Remove a child subscription from a composite when unsubscribing. */
    private static final class Remover implements Subscription {
        final Subscription s;
        final CompositeSubscription parent;
        volatile int once;
        static final AtomicIntegerFieldUpdater<Remover> ONCE_UPDATER
                = AtomicIntegerFieldUpdater.newUpdater(Remover.class, "once");

        public Remover(Subscription s, CompositeSubscription parent) {
            this.s = s;
            this.parent = parent;
        }

        @Override
        public boolean isUnsubscribed() {
            return s.isUnsubscribed();
        }

        @Override
        public void unsubscribe() {
            if (ONCE_UPDATER.compareAndSet(this, 0, 1)) {
                parent.remove(s);
            }
        }

    }
}
