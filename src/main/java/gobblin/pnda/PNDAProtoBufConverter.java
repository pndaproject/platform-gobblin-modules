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

import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.avro.Schema;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.SchemaConversionException;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.InvalidProtocolBufferException;

import gobblin.pnda.registry.ProtobufTopicConfig;

/**
 * An implementation of {@link Converter}.
 *
 * <p>
 *   This converter converts the input string schema into an
 *   Avro {@link org.apache.avro.Schema} and each input byte[] document
 *   into an Avro {@link org.apache.avro.generic.GenericRecord}.
 * </p>
 */
public class PNDAProtoBufConverter extends PNDAAbstractConverter<Map<FieldDescriptor, Object>, ProtobufTopicConfig> {

  private static final Logger log = LoggerFactory.getLogger(PNDAProtoBufConverter.class);
  private Descriptor pbDescriptor = null;
  private FieldDescriptor pbSource = null;
  private FieldDescriptor pbTimestamp = null;

  @Override
  public Schema convertSchema(String topic, WorkUnitState workUnit) throws SchemaConversionException {

    Schema schema = super.convertSchema(topic, workUnit);
    ProtobufTopicConfig protoConfig = getConfig();

    // Construct the proto message type
    DescriptorProto.Builder mMsgTypeBuilder = DescriptorProto.newBuilder();
    mMsgTypeBuilder.setName("Telemetry");

    if(protoConfig.hasSource()) {
      FieldDescriptorProto fb1 = buildField("node_id_str", protoConfig.getSourceTag(),
          FieldDescriptorProto.Label.LABEL_OPTIONAL, FieldDescriptorProto.Type.TYPE_STRING);
      mMsgTypeBuilder.addField(fb1);
    }
    if(protoConfig.hasTimeStamp()) {
      FieldDescriptorProto fb2 = buildField("msg_timestamp", protoConfig.getTimestampTag(),
          FieldDescriptorProto.Label.LABEL_OPTIONAL, FieldDescriptorProto.Type.TYPE_UINT64);
      mMsgTypeBuilder.addField(fb2);
    }

    // Construct the file proto file
    FileDescriptorProto.Builder pb = FileDescriptorProto.newBuilder();
    //pb.setSyntax("proto3");
    pb.addMessageType(mMsgTypeBuilder);
    FileDescriptorProto proto = pb.build();

    // Compile the descriptors
    try {
      FileDescriptor[] dependencies = {};
      FileDescriptor fileDesc = FileDescriptor.buildFrom(proto, dependencies);
      this.pbDescriptor = fileDesc.findMessageTypeByName("Telemetry");
      if (protoConfig.hasSource()) {
        this.pbSource = this.pbDescriptor.findFieldByNumber(protoConfig.getSourceTag());
      }
      if (protoConfig.hasTimeStamp()) {
        this.pbTimestamp = this.pbDescriptor.findFieldByNumber(protoConfig.getTimestampTag());
      }
    } catch (DescriptorValidationException error) {
      throw new SchemaConversionException(String.format("Unable to read AVRO schema file %s", error));
    }
    return schema;
  }

  Map<FieldDescriptor, Object> parse(byte[] inputRecord) throws QuarantineException {
    DynamicMessage dmsg;
    try {
      dmsg = DynamicMessage.parseFrom(this.pbDescriptor, inputRecord);
      // Extract the know fields
      Map<FieldDescriptor, Object> map = dmsg.getAllFields();
      return map;
    } catch (InvalidProtocolBufferException e) {
      log.error("InvalidProtocolBufferException: " + e);
      throw new QuarantineException("Unable to parse protobuf data: " + e);
    }
  }

  Object getTimeStamp(Map<FieldDescriptor, Object> map, ProtobufTopicConfig config) {
    Object timestamp = null;
    if (null != pbTimestamp) {
      timestamp = map.get(pbTimestamp);
      log.info("Extracted [" + pbTimestamp.getNumber() + "]: " + timestamp);
    }
    return timestamp;
  }

  Object getSource(Map<FieldDescriptor, Object> map, ProtobufTopicConfig config) {
    Object source = null;
    if (null != pbSource) {
      source = map.get(pbSource);
      log.info("Extracted [" + pbSource.getNumber() + "]: " + source);
    }
    return source;
  }

  private static FieldDescriptorProto buildField(String name, int tag, FieldDescriptorProto.Label label,
      FieldDescriptorProto.Type type) {
    FieldDescriptorProto.Builder fb = FieldDescriptorProto.newBuilder();
    fb.setType(type);
    fb.setLabel(label);
    fb.setName(name);
    fb.setNumber(tag);
    return fb.build();
  }
}
