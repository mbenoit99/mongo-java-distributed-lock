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
import com.deftlabs.lock.mongo.DistributedLockSvcOptions;

// Mongo
import com.mongodb.Mongo;
import org.bson.types.ObjectId;

// Java
import java.util.Map;

/**
 * The lock monitors.
 */
final class Monitor {

    /**
     * The lock heartbeat thread is responsible for sending updates to the lock
     * doc every X seconds (when the lock is owned by the current process). This
     * library uses missing/stale/old heartbeats to timeout locks that have not been
     * closed properly (based on the lock/unlock) contract. This can happen when processes
     * die unexpectedly (e.g., out of memory) or when they are not stopped properly (e.g., kill -9).
     */
    static class LockHeartbeat implements Runnable {
        @Override
        public void run() {
            while (_running) {
                try {
                    for (final String lockName : _locks.keySet()) {
                        final DistributedLock lock = _locks.get(lockName);

                        final ObjectId lockId = lock.getLockId();

                        if (!lock.isLocked() || lockId == null) continue;

                        LockDao.heartbeat(_mongo, lockName, lockId, lock.getOptions(), _svcOptions);
                    }

                    Thread.sleep(HEARTBEAT_FREQUENCY);
                } catch (final InterruptedException ie) { break;
                } catch (final Throwable t) {
                    t.printStackTrace();
                    // TOOD: Handle with global logger
                }
            }
        }

        LockHeartbeat(  final Mongo pMongo,
                        final DistributedLockSvcOptions pSvcOptions,
                        final Map<String, DistributedLock> pLocks)
        {
            _mongo = pMongo;
            _svcOptions = pSvcOptions;
            _locks = pLocks;
        }

        private static final long HEARTBEAT_FREQUENCY = 5000;

        void stopRunning() { _running = false; }
        private volatile boolean _running = true;
        private final Mongo _mongo;
        private final DistributedLockSvcOptions _svcOptions;
        private final Map<String, DistributedLock> _locks;
    }

    /**
     * The lock timeout thread impl (see LockHeartbeat docs for more info). One lock
     * timeout thread runs in each process this lock lib is running. This thread is
     * responsible for cleaning up expired locks (based on time since last heartbeat).
     */
    static class LockTimeout implements Runnable {
        @Override
        public void run() {
            while (_running) {
                try {

                    LockDao.expireInactiveLocks(_mongo, _svcOptions);

                    Thread.sleep(CHECK_FREQUENCY);
                } catch (final InterruptedException ie) { break;
                } catch (final Throwable t) {
                    t.printStackTrace();
                    // TOOD: Handle with global logger
                }
            }
        }

        LockTimeout(final Mongo pMongo,
                    final DistributedLockSvcOptions pSvcOptions)
        {
            _mongo = pMongo;
            _svcOptions = pSvcOptions;
        }

        private static final long CHECK_FREQUENCY = 60000;

        void stopRunning() { _running = false; }
        private volatile boolean _running = true;

        private final Mongo _mongo;
        private final DistributedLockSvcOptions _svcOptions;
    }
}
