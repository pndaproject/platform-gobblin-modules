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

public abstract class TopicConfig {

    private final String familyId;
    private static final Logger log = LoggerFactory.getLogger(TopicConfig.class);

    /*
     * Object to be returned by a registry to indicate a topic has no associated configuration.
     */ 
    public static final TopicConfig NOCONFIG = new TopicConfig(null, null) {
      public String getConverterClass() {return null;};
    };

    TopicConfig(String topic, String familyId) {
        if (null == familyId) {
            log.warn(String.format("Missing %s for topic %s, will use topic name instead", GobblinConfigRegistry.PNDA_FAMILY_ID, topic));
            this.familyId = topic;
        } else {
            this.familyId = familyId;
        }
    }

    /*
     * Returns the qualified path to the Converter class to be used for this topic.
     * Example: gobblin.pnda.PNDAAvroConverter
     */ 
    public abstract String getConverterClass();

    /* 
     * Returns the family ID associated to the topic.
     * If no family ID was configured, the topic name will be used instead.
     */
    public String getFamilyID() {return familyId;};

    /*
     * Indicates whether timestamp extraction information is available for the data family.
     */ 
    public boolean hasTimeStamp() {return false;};

    /*
     * Indicates whether source extraction information is available for the data family.
     */ 
    public boolean hasSource() {return false;};
    
}
