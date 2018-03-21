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

package gobblin.pnda;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


/*
 * This is a dummy implementation of the PNDARegistry.
 */
class PNDARegistry {

  static final TopicConfig NOCONFIG = new noconfig();

  static TopicConfig getConfig(String topic) {
      if (topic.startsWith("protobuf.example")) {
          return new ProtobufTopicConfig(1,10);
      } else if (topic.startsWith("protobuf.notime")) {
        return new ProtobufTopicConfig(1,-1);
      } else if (topic.startsWith("protobuf.nosource")) {
        return new ProtobufTopicConfig(-1,10);
      } else if (topic.startsWith("protobuf.nono")) {
        return new ProtobufTopicConfig(-1,-1);
      } else if (topic.startsWith("avro.example")) {
        return new AvroTopicConfig("src", "timestamp", pndaSchema);
      } else if (topic.startsWith("avro.notime")) {
        return new AvroTopicConfig("src", null, pndaSchema);
      } else if (topic.startsWith("avro.nosource")) {
        return new AvroTopicConfig(null, "timestamp", pndaSchema);
      } else if (topic.startsWith("avro.nono")) {
        return new AvroTopicConfig(null, null, pndaSchema);
      } else if (topic.startsWith("avro.1example")) {
        return new AvroTopicConfig("src1", "timestamp1", otherSchema);
      } else if (topic.startsWith("avro.1notime")) {
        return new AvroTopicConfig("src1", null, otherSchema);
      } else if (topic.startsWith("avro.1nosource")) {
        return new AvroTopicConfig(null, "timestamp1", otherSchema);
      } else if (topic.startsWith("avro.1nono")) {
        return new AvroTopicConfig(null, null, otherSchema);
      } else {
          return NOCONFIG;
      }
  }

  private static String pndaSchema = "{"+
    "\"namespace\": \"pnda.entity\","+
    "\"type\": \"record\","+
    "\"name\": \"event\","+
    "\"fields\": ["+
    "    {\"name\": \"timestamp\", \"type\": \"long\"},"+
    "    {\"name\": \"src\",       \"type\": \"string\"},"+
    "    {\"name\": \"host_ip\",   \"type\": \"string\"},"+
    "    {\"name\": \"rawdata\",   \"type\": \"bytes\"}"+
    "]"+
    "}";

    private static String otherSchema = "{"+
    "\"namespace\": \"pnda.entity\","+
    "\"type\": \"record\","+
    "\"name\": \"event\","+
    "\"fields\": ["+
    "    {\"name\": \"timestamp1\", \"type\": \"long\"},"+
    "    {\"name\": \"src1\",       \"type\": \"string\"},"+
    "    {\"name\": \"host_ip\",   \"type\": \"string\"},"+
    "    {\"name\": \"rawdata\",   \"type\": \"bytes\"}"+
    "]"+
    "}";

}

abstract class TopicConfig {

    public abstract String getConverterClass();

    public byte[] getFamilyID() {
        try {
          return MessageDigest.getInstance("MD5").digest(toString().getBytes("UTF-8"));
        } catch (UnsupportedEncodingException | NoSuchAlgorithmException e) {
            throw new IllegalStateException("Unexpected error trying to convert schema to bytes", e);
        }
    }
    public boolean hasTimeStamp() {return false;};
    public boolean hasSource() {return false;};
}

class noconfig extends TopicConfig {
    public String getConverterClass() {return null;};
}

class AvroTopicConfig extends TopicConfig {
    String timestampField = null;
    String sourceField = null;
    String schema = null;

    public AvroTopicConfig(String sourceField, String timestampField, String schema) {
      this.timestampField = timestampField;
      this.sourceField = sourceField;
      this.schema = schema;
    }
    

    public String getConverterClass() {
        return "gobblin.pnda.PNDAAvroConverter";
    }
    public String getTimestampField() {
        return timestampField;
    }
    public String getSourceField() {
        return sourceField;
    }

    public boolean hasSource() {return (null!=getSourceField());}
    public boolean hasTimeStamp() {return (null!=getTimestampField());}
    public String getSchema() { return schema;}
    public String toString() {
        return getConverterClass()+getTimestampField()+getSourceField();
    }
}

class ProtobufTopicConfig extends TopicConfig {

    int timestampTag;
    int sourceTag;

    public ProtobufTopicConfig(int sourceTag, int timestampTag) {
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
    public boolean hasSource() {return (-1!=getSourceTag());}
    public boolean hasTimeStamp() {return (-1!=getTimestampTag());}
    public String toString() {
        return getConverterClass()+getTimestampTag()+getSourceTag();
    }
  }
