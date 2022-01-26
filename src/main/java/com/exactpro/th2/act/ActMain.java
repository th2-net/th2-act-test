/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.act;

import static com.exactpro.th2.common.metrics.CommonMetrics.LIVENESS_MONITOR;
import static com.exactpro.th2.common.metrics.CommonMetrics.READINESS_MONITOR;

import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.act.impl.SubscriptionManagerImpl;
import com.exactpro.th2.check1.grpc.Check1Service;
import com.exactpro.th2.common.grpc.MessageBatch;
import com.exactpro.th2.common.schema.factory.CommonFactory;
import com.exactpro.th2.common.schema.grpc.router.GrpcRouter;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;

public class ActMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(ActMain.class);
    private static final String OE_ATTRIBUTE_NAME = "oe";

    public static void main(String[] args) {
        Deque<AutoCloseable> resources = new ConcurrentLinkedDeque<>();
        ReentrantLock lock = new ReentrantLock();
        Condition condition = lock.newCondition();

        configureShutdownHook(resources, lock, condition);
        try {
            LIVENESS_MONITOR.enable();
            CommonFactory factory = CommonFactory.createFromArguments(args);
            resources.add(factory);

            GrpcRouter grpcRouter = factory.getGrpcRouter();
            resources.add(grpcRouter);

            MessageRouter<MessageBatch> messageRouter = factory.getMessageRouterParsedBatch();
            resources.add(messageRouter);

            SubscriptionManagerImpl subscriptionManager = new SubscriptionManagerImpl();
            SubscriberMonitor subscriberMonitor = messageRouter.subscribeAll(subscriptionManager, OE_ATTRIBUTE_NAME);
            resources.add(subscriberMonitor::unsubscribe);

            ActHandler actHandler = new ActHandler(
                    messageRouter,
                    subscriptionManager,
                    factory.getEventBatchRouter(),
                    grpcRouter.getService(Check1Service.class)
            );
            ActServer actServer = new ActServer(grpcRouter.startServer(actHandler));
            resources.add(actServer::stop);
            READINESS_MONITOR.enable();
            LOGGER.info("Act started");
            awaitShutdown(lock, condition);
        } catch (InterruptedException e) {
            LOGGER.info("The main thread interupted", e);
        } catch (Exception e) {
            LOGGER.error("Fatal error: {}", e.getMessage(), e);
            System.exit(1);
        }
    }

    private static void awaitShutdown(ReentrantLock lock, Condition condition) throws InterruptedException {
        try {
            lock.lock();
            LOGGER.info("Wait shutdown");
            condition.await();
            LOGGER.info("App shutdowned");
        } finally {
            lock.unlock();
        }
    }

    private static void configureShutdownHook(Deque<AutoCloseable> resources, ReentrantLock lock, Condition condition) {
        Runtime.getRuntime().addShutdownHook(new Thread("Shutdown hook") {
            @Override
            public void run() {
                LOGGER.info("Shutdown start");
                READINESS_MONITOR.disable();
                try {
                    lock.lock();
                    condition.signalAll();
                } finally {
                    lock.unlock();
                }

                resources.descendingIterator().forEachRemaining(resource -> {
                    try {
                        resource.close();
                    } catch (Exception e) {
                        LOGGER.error(e.getMessage(), e);
                    }
                });
                LIVENESS_MONITOR.disable();
                LOGGER.info("Shutdown end");
            }
        });
    }
}
