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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetNotFoundException;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Datasets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.util.EmptyIterable;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.DataConversionException;
import gobblin.converter.SingleRecordIterable;

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
public abstract class PNDAAbstractConverter<D, C extends TopicConfig>
    extends Converter<String, Schema, byte[], GenericRecord> {

  private static final Logger log = LoggerFactory.getLogger(PNDAAbstractConverter.class);

  /* Error handling attributes */
  public static final String KITE_ERROR_DATASET_URI = "PNDA.quarantine.dataset.uri";
  public static final String TIMESTAMP_FIELD = "timestamp";
  public static final String SOURCE_FIELD = "source";
  public static final String TIMESTAMP_PROPERTY = "pnda.field.timestamp.extracted";
  public static final String SOURCE_PROPERTY = "pnda.field.source.extracted";
  public static final String FAMILY_ID_PROPERTY = "pnda.family.id";
  public static final String PNDA_CONVERTER_SCHEMA = "PNDA.converter.schema";

  private long loggedErrors;
  public static final long MAX_LOGGED_ERRORS = 10;
  private DatasetWriter<GenericRecord> errorWriter = null;
  private Schema errorSchema = null;

  protected Schema outputSchema = null;
  protected String topic = null;
  private C config = null;

  protected C getConfig() {
    return config;
  }

  public PNDAAbstractConverter<D, C> init(WorkUnitState workUnit, C config) {
    this.config = config;
    setProp(workUnit, TIMESTAMP_PROPERTY, Boolean.valueOf(config.hasTimeStamp()));
    setProp(workUnit, SOURCE_PROPERTY, Boolean.valueOf(config.hasSource()));
    setProp(workUnit, FAMILY_ID_PROPERTY, config.getFamilyID());
    this.loggedErrors = 0;
    String errorDatasetUri = workUnit.getProp(KITE_ERROR_DATASET_URI);
    if (errorDatasetUri != null) {
      try {
        Dataset<GenericRecord> quarantine = Datasets.load(errorDatasetUri);
        this.errorSchema = quarantine.getDescriptor().getSchema();
        this.errorWriter = quarantine.newWriter();
      } catch (DatasetNotFoundException error) {
        log.error(String.format("Unable to load Quarantine Dataset at %s. Bad data will be ignored", errorDatasetUri));
      }
    } else {
      log.error(
          String.format("'%s' configuration property not set. Bad data " + "will be ignored", KITE_ERROR_DATASET_URI));
    }
    log.info(String.format("Messages that are not in the PNDA format will " + "be written to '%s'", errorDatasetUri));
    return this;
  }

  public void close() throws IOException {
    if (errorWriter != null) {
      errorWriter.close();
    }
  }

  @Override
  public Schema convertSchema(String topic, WorkUnitState workUnit) throws SchemaConversionException {
    /*
     * In our case, the source is a KafkaSimpleExtractor which give the topic
     * name as the inputSchema, this is not a AVRO schema.
     */
    this.topic = topic;
    return this.outputSchema = parseSchema(workUnit.getProp(PNDA_CONVERTER_SCHEMA));
  }

  protected Schema parseSchema(String sourceSchema) throws SchemaConversionException {
    Schema schema = null;
    if (sourceSchema == null || sourceSchema.isEmpty()) {
      throw new IllegalArgumentException(
          String.format("Schema configuration parameter missing or empty"));
    }
    if (sourceSchema.startsWith("{")) {
      log.info(String.format("Using the following AVRO schema: %s", sourceSchema));
      schema = new Schema.Parser().parse(sourceSchema);
    } else {
      log.info(String.format("Using the following AVRO schema file: %s", sourceSchema));
      try {
        new Schema.Parser().parse(new File(sourceSchema));
      } catch (IOException error) {
        throw new SchemaConversionException(String.format("Unable to read AVRO schema file %s", error));
      }
    }
    return schema;
  }

  protected void writeErrorData(byte[] inputRecord, String reason) {
    if (errorWriter == null) {
      return;
    }

    /* Only log at most MAX_LOGGED_ERRORS messages */
    loggedErrors++;
    if (loggedErrors < MAX_LOGGED_ERRORS) {
      log.error(
          String.format("A record from topic '%s' was not deserizable," + "it was put in quarantine", this.topic));
    } else if (loggedErrors == MAX_LOGGED_ERRORS) {
      log.error(String.format("Stopping logging deserialization errors " + "after %d messages", MAX_LOGGED_ERRORS));
    }
    GenericRecord errorRecord = new GenericData.Record(errorSchema);

    errorRecord.put("topic", topic);
    errorRecord.put("timestamp", System.currentTimeMillis());
    errorRecord.put("reason", reason);
    errorRecord.put("payload", inputRecord);

    this.errorWriter.write(errorRecord);
  }

  @Override
  public Iterable<GenericRecord> convertRecord(Schema schema, byte[] inputRecord, WorkUnitState workUnit)
      throws DataConversionException {

    D parsedRecord = null;
    try {
      parsedRecord = parse(inputRecord);
    } catch (QuarantineException e) {
      writeErrorData(inputRecord, e.getReason());
    }

    if (null == parsedRecord) {
      // Data has been quaranteened, we are done here.
      return new EmptyIterable<GenericRecord>();
    }

    Object rSource = getSource(parsedRecord, config);
    rSource = (null == rSource) ? topic : rSource;
    Object rTimestamp = getTimeStamp(parsedRecord, config);
    rTimestamp = (null == rTimestamp) ? Long.valueOf(System.currentTimeMillis()) : rTimestamp;

    GenericRecord record = new GenericData.Record(outputSchema);
    record.put(TIMESTAMP_FIELD, rTimestamp);
    record.put(SOURCE_FIELD, rSource);
    record.put("rawdata", ByteBuffer.wrap(inputRecord));
    return new SingleRecordIterable<GenericRecord>(record);
  }

  /** 
   * Parses an input data record to an output data record for future data extraction
   * @param inputRecord the input data record to be parsed
   * @return the parsed output data record 
   */
  abstract D parse(byte[] inputRecord) throws QuarantineException;

  /**
   * Extracts the timestamp data from the parsed input data record
   * @param record the input data record
   * @param config the configuration associated to the data
   * @return the timestamp data extracted from the input data record, 
   *         in millis since the epoch (1970-01-01 UTC).
   */
  abstract Object getTimeStamp(D record, C config);

  /**
   * Extracts the source data from the parsed input data record
   * @param record the input data record
   * @param config the configuration associated to the data
   * @return the source data extracted from the input data record
   */
  abstract Object getSource(D record, C config);

  private static void setProp(WorkUnitState workUnit, String key, Object value) {
    if (null == value) return;

    workUnit.setProp(key, value);
    String props = workUnit.getProp(AvroHdfsDataWriter.WRITER_FILE_AVRO_METADATA);
    // Add this key to the list of properties to be witten in the Avro meta data header.
    workUnit.setProp(AvroHdfsDataWriter.WRITER_FILE_AVRO_METADATA, (null == props) ? key : props+","+key);
  }
}

class QuarantineException extends Exception {
  private static final long serialVersionUID = -6103104588164917107L;
  private final String reason;

  QuarantineException(String reason) {
    this.reason = reason;
  }

  /**
   * @return the reason
   */
  public String getReason() {
    return reason;
  }

}
