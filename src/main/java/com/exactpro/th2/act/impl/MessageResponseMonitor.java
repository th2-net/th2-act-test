/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.act.impl;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.act.ResponseMonitor;

public final class MessageResponseMonitor implements ResponseMonitor {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageResponseMonitor.class);
    private final Lock responseLock = new ReentrantLock();
    private final Condition responseReceivedCondition = responseLock.newCondition();
    private boolean notified;

    public void awaitSync(long timeout, TimeUnit timeUnit) throws InterruptedException {
        try {
            responseLock.lock();
            if (notified) {
                LOGGER.info("Monitor has been notified before it has started to await a response");
                return;
            }
            if (!responseReceivedCondition.await(timeout, timeUnit)) {
                LOGGER.info("Timeout elapsed before monitor was notified");
            }
        } finally {
            responseLock.unlock();
        }
    }

    @Override
    public void responseReceived() {
        try {
            responseLock.lock();
            responseReceivedCondition.signalAll();
            notified = true;
        } finally {
            responseLock.unlock();
        }
    }
}
