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

import org.apache.avro.Schema;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.SchemaConversionException;

import gobblin.pnda.registry.TopicConfig;


/**
 * An implementation of {@link Converter}.
 *
 * <p>
 *   This converter converts the input string schema into an
 *   Avro {@link org.apache.avro.Schema} and each input byte[] document
 *   into an Avro {@link org.apache.avro.generic.GenericRecord}.
 * </p>
 */
public class PNDAFallbackConverter extends PNDAAbstractConverter<Object, TopicConfig> {

  public void close() throws IOException {
    super.close();
  }

  @Override
  public Schema convertSchema(String inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    return super.convertSchema(inputSchema, workUnit);
  }

  Object parse(byte[] inputRecord) {
    return new Object();
  }

  Object getTimeStamp(Object record, TopicConfig config) {
    return null;
  }

  Object getSource(Object record, TopicConfig config) {
    return null;
  }

}
