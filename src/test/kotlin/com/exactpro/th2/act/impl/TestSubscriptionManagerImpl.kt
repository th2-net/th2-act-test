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
package com.exactpro.th2.act.impl

import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.MessageBatch
import com.exactpro.th2.common.message.message
import com.exactpro.th2.common.schema.message.MessageListener
import com.nhaarman.mockitokotlin2.*
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

class TestSubscriptionManagerImpl {

    private val manager = SubscriptionManagerImpl()

    @ParameterizedTest
    @EnumSource(value = Direction::class, mode = EnumSource.Mode.EXCLUDE, names = ["UNRECOGNIZED"])
    fun `correctly distributes the batches`(direction: Direction) {
        val listeners: Map<Direction, MessageListener<MessageBatch>> = mapOf(
                Direction.FIRST to mock { },
                Direction.SECOND to mock { }
        )
        listeners.forEach { (dir, listener) -> manager.register(dir, listener) }

        val batch = MessageBatch.newBuilder()
                .addMessages(message("test", direction, "test"))
                .build()
        manager.handler("", batch)

        Assertions.assertAll(
                listeners.map { (dir, listener) ->
                    Executable {
                        if (dir == direction) {
                            verify(listener).handler(any(), same(batch))
                        } else {
                            verify(listener, never()).handler(any(), any())
                        }
                    }
                }
        )
    }

    @ParameterizedTest
    @EnumSource(value = Direction::class, mode = EnumSource.Mode.EXCLUDE, names = ["UNRECOGNIZED"])
    fun `removes listener`(direction: Direction) {
        val listener = mock<MessageListener<MessageBatch>> { }
        manager.register(direction, listener)


        manager.unregister(direction, listener)

        val batch = MessageBatch.newBuilder()
                .addMessages(message("test", direction, "test"))
                .build()
        manager.handler("", batch)

        verify(listener, never()).handler(any(), any())
    }
}