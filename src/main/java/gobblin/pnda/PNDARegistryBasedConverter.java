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
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link Converter}.
 *
 * <p>
 *   This converter converts the input string schema into an
 *   Avro {@link org.apache.avro.Schema} and each input byte[] document
 *   into an Avro {@link org.apache.avro.generic.GenericRecord}.
 * </p>
 */
public class PNDARegistryBasedConverter extends Converter<String,
                                                    Schema,
                                                    byte[],
                                                    GenericRecord> {

  private PNDAAbstractConverter delegate = null;
  private static final Logger log = LoggerFactory.getLogger(PNDARegistryBasedConverter.class);

  public PNDARegistryBasedConverter init(WorkUnitState workUnit) {
    return this;
  }

  public void close() throws IOException {
    delegate.close();
  }

  @Override
  public Schema convertSchema(String inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {

      TopicConfig config = PNDARegistry.getConfig(inputSchema);
      String className = (config==null)?null:config.getConverterClass();
      if(null != className) {
        try {
          Class<?> clazz = Class.forName(className);
          Constructor<?> ctor = clazz.getConstructor();
          delegate = (PNDAAbstractConverter)ctor.newInstance();
          log.info("Setting delegate Converter to "+className);
        } catch (ClassNotFoundException | InstantiationException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
          throw new SchemaConversionException(
            String.format("Failed to instantiate delegate class '%s': %s", className, e));
        }
      } else {
        // If no Converter is defined, just wrap everything in the PNDA envelop
        // without extracting any values from the input.
        delegate = new PNDAFallbackConverter();
        log.info("Setting delegate Converter to PNDAFallbackConverter");
      }
      delegate.init(workUnit, config);
      return delegate.convertSchema(inputSchema, workUnit);
}

  @Override
  public Iterable<GenericRecord> convertRecord(Schema schema, byte[] inputRecord,
                                               WorkUnitState workUnit)
      throws DataConversionException {
        return delegate.convertRecord(schema, inputRecord, workUnit);
  }
}
