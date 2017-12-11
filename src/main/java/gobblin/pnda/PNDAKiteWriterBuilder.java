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
import org.apache.avro.generic.GenericRecord;

import org.kitesdk.data.Dataset;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.DatasetNotFoundException;

import gobblin.configuration.State;
import gobblin.writer.DataWriter;
import gobblin.writer.DataWriterBuilder;

import gobblin.pnda.PNDAKiteWriter;

/**
 * A {@link DataWriterBuilder} for building {@link DataWriter} that writes in Kite data.
 */
@SuppressWarnings("unused")
public class PNDAKiteWriterBuilder extends DataWriterBuilder<Schema, GenericRecord> {
  public static final String KITE_WRITER_DATASET_URI = "kite.writer.dataset.uri";

  @Override
  public final DataWriter<GenericRecord> build() throws IOException {
      State properties = this.destination.getProperties();

      String datasetURI = properties.getProp(KITE_WRITER_DATASET_URI);
      try {
          Dataset events = Datasets.load(datasetURI);
          return new PNDAKiteWriter(properties, events);
      } catch (DatasetNotFoundException error) {
          throw new RuntimeException(
                          String.format("Unable to read dataset '%s'", datasetURI), error);
      }
  }
}
