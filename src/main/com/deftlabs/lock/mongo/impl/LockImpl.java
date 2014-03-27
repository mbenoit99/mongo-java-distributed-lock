/**
 * Copyright 2011, Deft Labs.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.deftlabs.lock.mongo.impl;

// Lib
import com.deftlabs.lock.mongo.DistributedLock;
import com.deftlabs.lock.mongo.DistributedLockOptions;
import com.deftlabs.lock.mongo.DistributedLockSvcOptions;

// Mongo
import com.mongodb.Mongo;
import org.bson.types.ObjectId;

// Java
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.LockSupport;

/**
 * The distributed lock object. Note that this lock implementation is not re-entrant. Once you
 * acquire the lock, an attempt by the same thread to re-acquire it by calling {@code lock()}
 * will result in a deadlock. Additionally, {@code tryLock()} will return {@code false} even when
 * invoked by the thread that currently holds the lock.
 */
public class LockImpl implements DistributedLock {

    /**
     * Construct the object with params.
     */
    LockImpl(   final Mongo pMongo,
                final String pName,
                final DistributedLockOptions pLockOptions,
                final DistributedLockSvcOptions pSvcOptions)
    {
        _mongo = pMongo;
        _name = pName;
        _lockOptions = pLockOptions;
        _svcOptions = pSvcOptions;
    }

    /**
     * Blocks until the distributed lock is available. Note that this lock is not re-entrant.
     * Once a thread acquires the lock, it should NOT call lock() again prior to unlocking,
     * otherwise it will deadlock itself.
     */
    @Override public void lock() {
        if (tryDistributedLock()) return;
        park();
    }

    /**
     * Park the current thread. This method will check to see (when allowed) to see
     * if it can get the distributed lock.
     */
    private void park() {

        boolean wasInterrupted = false;
        final Thread current = Thread.currentThread();
        _waitingThreads.add(current);

        // Block while not first in queue or cannot acquire lock
        while (_running.get()) {

            LockSupport.park(this);

            if (Thread.interrupted()) { wasInterrupted = true; break; }

            if (_waitingThreads.peek() == current && isUnlocked()) {
                // Check to see if this thread can get the distributed lock
                if (tryDistributedLock()) break;
            }
        }

        // We can't just remove the head here, because the current thread may not be
        // at the head of the queue (ie, if the thread was interrupted). Thus find and remove
        // the current thread from the queue.
        _waitingThreads.remove(current);
        if (wasInterrupted) current.interrupt();
    }

    /**
     * Park the current thread for a max amount of time. This method will check to see
     * (when allowed) to see if it can get the distributed lock.
     */
    private boolean park(final long pNanos) {

        boolean wasInterrupted = false;
        final Thread current = Thread.currentThread();
        _waitingThreads.add(current);

        boolean locked = false;

        long parkTime = pNanos;

        long startTime = System.nanoTime();

        // Block while not first in queue or cannot acquire lock
        while (_running.get()) {

            parkTime = (pNanos - (System.nanoTime() - startTime));

            if (parkTime <= 0) break;

            LockSupport.parkNanos(this, parkTime);

            if (Thread.interrupted()) { wasInterrupted = true; break; }

            if (_waitingThreads.peek() == current && isUnlocked()) {
                // Check to see if this thread can get the distributed lock
                if (tryDistributedLock()) { locked = true; break; }
            }

            if ((System.nanoTime() - startTime) >= pNanos) break;
        }

        // We can't just remove the head here, because the current thread may not be
        // at the head of the queue (ie, if the park() timed out). Thus find and remove
        // the current thread from the queue.
        _waitingThreads.remove(current);
        if (wasInterrupted) current.interrupt();
        return locked;
    }

    /**
     * Try and lock the distributed lock.
     */
    private boolean tryDistributedLock() {
        final ObjectId lockId = LockDao.lock(_mongo, _name, _svcOptions, _lockOptions);

        if (lockId == null) return false;

        _locked.set(true);
        _lockId = lockId;
        _lastHeartbeat = System.currentTimeMillis();
        
        return true;
    }

    /**
     * This is not supported.
     */
    @Override public void lockInterruptibly()
    { throw new UnsupportedOperationException("not implemented"); }

    /**
     * For now, this is not supported.
     */
    @Override public Condition newCondition()
    { throw new UnsupportedOperationException("not implemented"); }

    /**
     * Does not block. Returns right away if not able to lock.
     */
    @Override public boolean tryLock() {
        if (isLockedLocally()) return false;
        return tryDistributedLock();
    }

    @Override public boolean tryLock(final long pTime, final TimeUnit pTimeUnit) {
        if (isLockedLocally()) return false;
        if (tryDistributedLock()) return true;
        return park(pTimeUnit.toNanos(pTime));
    }

    @Override public void unlock() {
        try {
            LockDao.unlock(_mongo, _name, _svcOptions, _lockOptions, _lockId);
        }
        finally {
            // Even if the DB update fails, we still want to re-initialize the lock
            // state to unlock it locally. Otherwise a DB failure would leave this
            // lock locked locally until the process dies. Also, the lock timeout will
            // ensure that a failed DB unlock will eventually release the lock.
            clear();
            LockSupport.unpark(_waitingThreads.peek());
        }
    }

    /**
     * Called to initialize the lock.
     */
    synchronized void init() {
        if (_running.get()) throw new IllegalStateException("init already called");
        _running.set(true);
    }

    /**
     * Called to destroy the lock.
     */
    synchronized void destroy() {
        if (!_running.get()) throw new IllegalStateException("destroy already called");
        _running.set(false);
        for (final Thread t : _waitingThreads) t.interrupt();
        _waitingThreads.clear();
    }

    /**
     * Returns true if the lock is currently locked. If the lock's heartbeat is too old,
     * attempt to re-acquire the distributed lock. When a long-running process is holding
     * the lock, it should periodically invoke isLocked() and stop whatever it's doing if
     * it no longer holds the lock. Be aware that this method has a side effect: if we
     * think we have the lock and then realize it was lost, this lock will be marked as
     * unlocked. As a result, if you lose a lock that you had previously acquired, you
     * should *not* attempt to unlock it. Doing so will clear out the lock state for
     * a thread that may have since acquired the lock.
     */
    @Override public boolean isLocked() {
        if (isUnlocked()) {
            return false;
        }
        if (!isHeartbeatTooOld()) {
            return true;
        }
        final boolean gotLockBack = tryDistributedLock();
        if (!gotLockBack) {
            // Couldn't get the lock back, so clear it out. Although we are effectively
            // unlocking the lock here, there is no need to invoke these two operations:
            // LockSupport.unpark() - we already know the distributed lock is not
            //   currently available, so any waiting threads should keep waiting.
            // LockDao.unlock() - another process has acquired the lock at this point,
            //   so invoking the DAO's unlock() method would be disastrous.
            clear();
        }
        return gotLockBack;
    }
    
    /**
     * Returns true if this lock is being held by the current process. It's possible that
     * the lock may have timed out (ie, due to a failure to update the heartbeat), in which
     * case another process may have acquired the lock.
     */
    private boolean isLockedLocally() {
        return _locked.get() == true;
    }
    
    private boolean isUnlocked() {
        return _locked.get() == false;
    }

    /**
     * Returns the lock name.
     */
    @Override public String getName() { return _name; }

    @Override public ObjectId getLockId() { return _lockId; }

    /**
     * Returns the options used to configure this lock.
     */
    @Override public DistributedLockOptions getOptions() { return _lockOptions; }

    /**
     * Wakeup any blocked threads. This should <b>ONLY</b> be used by the lock service,
     * not the user.
     */
    @Override public void wakeupBlocked() { LockSupport.unpark(_waitingThreads.peek()); }
    
    @Override public void setLastHeartbeat(final long pWhen) {
        _lastHeartbeat = pWhen;
    }
    
    /*
     * Checks whether the lock has timed out since the last heartbeat was reported.
     */
    private boolean isHeartbeatTooOld() {
        final long age = System.currentTimeMillis() - _lastHeartbeat;
        return age > _lockOptions.getInactiveLockTimeout();
    }
    
    /**
     * Clears out all locking state.
     */
    private void clear() {
        _locked.set(false);
        _lockId = null;
        _lastHeartbeat = 0;
    }

    @Override
    public String toString() {
        return String.format(
            "[%d / %s] lastHeartbeat=%s, lockId=%s, locked=%s, numWaiters=%d",
            hashCode(),
            _name,
            _lastHeartbeat,
            _lockId,
            _locked.get(),
            _waitingThreads.size());
    }
    
    private final String _name;
    private final Mongo _mongo;
    private final DistributedLockOptions _lockOptions;
    private final DistributedLockSvcOptions _svcOptions;

    private volatile long _lastHeartbeat = 0;
    private volatile ObjectId _lockId;
    private final AtomicBoolean _locked = new AtomicBoolean(false);
    private final AtomicBoolean _running = new AtomicBoolean(false);
    private final Queue<Thread> _waitingThreads = new ConcurrentLinkedQueue<Thread>();
}

