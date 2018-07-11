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

public class ProtobufTopicConfig extends TopicConfig {

    int timestampTag;
    int sourceTag;

    static final int NO_SOURCE = -1;
    static final int NO_TIMESTAMP = -1;

    public ProtobufTopicConfig(String topic, String familyId, int sourceTag, int timestampTag) {
        super(topic, familyId);
        this.sourceTag = sourceTag;
        this.timestampTag = timestampTag;
    }

    public String getConverterClass() {
        return "gobblin.pnda.PNDAProtoBufConverter";
    }
    public int getTimestampTag() {
        return timestampTag;
    }
    public int getSourceTag() {
        return sourceTag;
    }
    public boolean hasSource() { return -1 != getSourceTag(); }
    public boolean hasTimeStamp() { return -1 != getTimestampTag(); }
    public String toString() {
        return getConverterClass() + getTimestampTag() + getSourceTag();
    }
}

