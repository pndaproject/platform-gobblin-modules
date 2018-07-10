/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gobblin.pnda.registry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.configuration.WorkUnitState;

public class GobblinConfigRegistry {

    // Common topic config
    static final String PNDA_CONVERTER_DELEGATE_CLASS = "pnda.converter.delegate.class";
    static final String PNDA_FAMILY_ID = "pnda.family.id";

    // Avro specific topic config
    static final String PNDA_AVRO_SOURCE_FIELD = "pnda.avro.source.field";
    static final String PNDA_AVRO_TIMESTAMP_FIELD = "pnda.avro.timestamp.field";
    static final String PNDA_AVRO_SCHEMA = "pnda.avro.schema";

    // ProtoBuffer specific topic config
    static final String PNDA_PROTOBUF_SOURCE_TAG = "pnda.protobuf.source.tag"; 
    static final String PNDA_PROTOBUF_TIMESTAMP_TAG = "pnda.protobuf.timestamp.tag"; 

    private static final Logger log = LoggerFactory.getLogger(GobblinConfigRegistry.class);

    public static TopicConfig getConfig(String topic, WorkUnitState properties) {
        String delegateName = properties.getProp(PNDA_CONVERTER_DELEGATE_CLASS);
        String familyId = properties.getProp(PNDA_FAMILY_ID);
        TopicConfig config = TopicConfig.NOCONFIG;
        if (null != delegateName) {
            // If there is a config, then the familyId is mandatory.
            switch (delegateName) {
                case "gobblin.pnda.PNDAAvroConverter":
                    config = new AvroTopicConfig(topic, familyId,
                            properties.getProp(PNDA_AVRO_SOURCE_FIELD),
                            properties.getProp(PNDA_AVRO_TIMESTAMP_FIELD),
                            properties.getProp(PNDA_AVRO_SCHEMA));
                    break;
                case "gobblin.pnda.PNDAProtoBufConverter":
                    int sourceTag = ProtobufTopicConfig.NO_SOURCE;
                    try {
                        sourceTag = Integer.parseInt(properties.getProp(PNDA_PROTOBUF_SOURCE_TAG));
                    } catch (NumberFormatException e) {
                        log.error("NumberFormatException when parsing PNDA_PROTOBUF_SOURCE_TAG: " + e);
                    }
                    int timestampTag = ProtobufTopicConfig.NO_TIMESTAMP;
                    try {
                        timestampTag = Integer.parseInt(properties.getProp(PNDA_PROTOBUF_TIMESTAMP_TAG));
                    } catch (NumberFormatException e) {
                        log.error("NumberFormatException when parsing PNDA_PROTOBUF_TIMESTAMP_TAG: " + e);
                    }
                    config = new ProtobufTopicConfig(topic, familyId, sourceTag, timestampTag);
                    break;
                default:
                	// TopicConfig.NOCONFIG;
                	break;
            }
        } else {
            config = new FallbackTopicConfig(topic, familyId);
        }
        return config;
    }
}

