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

import static com.google.protobuf.TextFormat.shortDebugString;

import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.act.SubscriptionManager;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.MessageBatch;
import com.exactpro.th2.common.schema.message.MessageListener;

public class SubscriptionManagerImpl implements MessageListener<MessageBatch>, SubscriptionManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionManagerImpl.class);
    private final Map<Direction, List<MessageListener<MessageBatch>>> callbacks;

    public SubscriptionManagerImpl() {
        Map<Direction, List<MessageListener<MessageBatch>>> callbacks = new EnumMap<>(Direction.class);
        callbacks.put(Direction.FIRST, new CopyOnWriteArrayList<>());
        callbacks.put(Direction.SECOND, new CopyOnWriteArrayList<>());
        this.callbacks = Collections.unmodifiableMap(callbacks);
    }

    @Override
    public void register(Direction direction, MessageListener<MessageBatch> listener) {
        List<MessageListener<MessageBatch>> listeners = getMessageListeners(direction);
        listeners.add(listener);
    }

    @Override
    public boolean unregister(Direction direction, MessageListener<MessageBatch> listener) {
        List<MessageListener<MessageBatch>> listeners = getMessageListeners(direction);
        return listeners.remove(listener);
    }

    @Override
    public void handler(String s, MessageBatch messageBatch) {
        if (messageBatch.getMessagesCount() < 0) {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("Empty batch received {}", shortDebugString(messageBatch));
            }
            return;
        }
        Direction direction = messageBatch.getMessages(0).getMetadata().getId().getDirection();
        List<MessageListener<MessageBatch>> listeners = callbacks.get(direction);
        if (listeners == null) {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("Unsupported direction {}. Batch: {}", direction, shortDebugString(messageBatch));
            }
            return;
        }

        for (MessageListener<MessageBatch> listener : listeners) {
            try {
                listener.handler(s, messageBatch);
            } catch (Exception e) {
                if (LOGGER.isErrorEnabled()) {
                    LOGGER.error("Cannot handle batch from {}. Batch: {}", direction, shortDebugString(messageBatch), e);
                }
            }
        }
    }

    private List<MessageListener<MessageBatch>> getMessageListeners(Direction direction) {
        List<MessageListener<MessageBatch>> listeners = callbacks.get(direction);
        if (listeners == null) {
            throw new IllegalArgumentException("Unsupported direction " + direction);
        }
        return listeners;
    }
}
