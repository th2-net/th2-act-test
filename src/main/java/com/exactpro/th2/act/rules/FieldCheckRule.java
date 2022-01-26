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
package com.exactpro.th2.act.rules;

import static com.google.protobuf.TextFormat.shortDebugString;

import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageOrBuilder;
import com.exactpro.th2.common.grpc.Value;

public class FieldCheckRule extends AbstractSingleConnectionRule {
    private static final Logger LOGGER = LoggerFactory.getLogger(FieldCheckRule.class);
    private final String expectedFieldValue;
    private final Map<String, String> msgTypeToFieldName;

    public FieldCheckRule(String expectedFieldValue, Map<String, String> msgTypeToFieldName, ConnectionID requestConnId) {
        super(requestConnId);
        this.expectedFieldValue = expectedFieldValue;

        msgTypeToFieldName.forEach((msgType, fieldName) -> {
            if (StringUtils.isAnyBlank(msgType, fieldName)) {
                throw new IllegalArgumentException("'msgTypeToFieldName' mapping must not contain blank values. "
                        + "MsgType: '" + msgType + "'"
                        + "FieldName: '" + fieldName + "'"
                );
            }
        });
        this.msgTypeToFieldName = msgTypeToFieldName;
    }

    @Override
    protected boolean checkMessageFromConnection(Message message) {
        String messageType = message.getMetadata().getMessageType();
        String fieldName = msgTypeToFieldName.get(messageType);
        if (fieldName != null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Checking the message: {}", shortDebugString(message));
            }
            if (checkExpectedField(message, fieldName)) {
                LOGGER.debug("FixCheckRule passed on {} messageType", messageType);
                return true;
            }
        }
        return false;
    }

    private boolean checkExpectedField(MessageOrBuilder message, String fieldName) {
        Value value = message.getFieldsMap().get(fieldName);
        return value != null
                && Objects.equals(expectedFieldValue, value.getSimpleValue());
    }
}
