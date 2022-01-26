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

package com.exactpro.th2.act.rules;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.Message;

public class MessagePropertiesCheckRule extends AbstractSingleConnectionRule {
    private final Map<String, String> expectedProperties;

    public MessagePropertiesCheckRule(ConnectionID requestConnId, Map<String, String> expectedProperties) {
        super(requestConnId);
        this.expectedProperties = Objects.requireNonNull(expectedProperties, "'Expected properties' parameter");
        if (expectedProperties.isEmpty()) {
            throw new IllegalArgumentException("At least one property must be specified in expected properties");
        }
    }

    @Override
    protected boolean checkMessageFromConnection(Message message) {
        Map<String, String> propertiesMap = message.getMetadata().getPropertiesMap();
        for (Entry<String, String> entry : expectedProperties.entrySet()) {
            String expectedKey = entry.getKey();
            String expectedValue = entry.getValue();
            if (!expectedValue.equals(propertiesMap.get(expectedKey))) {
                return false;
            }
        }
        return true;
    }
}
