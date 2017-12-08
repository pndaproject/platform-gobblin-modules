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

import org.apache.avro.generic.GenericRecord;

import com.google.common.base.Preconditions;

import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetWriter;

import gobblin.configuration.State;

import gobblin.writer.DataWriter;

/**
 * Implementation of {@link DataWriter} that writes data with the
 * KiteSDK library
 * Every partitioning, format handling etc ... is done with the
 * Kite library
 */

public class PNDAKiteWriter implements DataWriter<GenericRecord> {

    protected final State properties;

    private int recordsWritten;
    private int bytesWritten;

    private DatasetWriter writer;

    public PNDAKiteWriter(State props, Dataset dataset) {
        this.properties = props;

        this.recordsWritten = 0;
        this.bytesWritten = 0;

        this.writer = dataset.newWriter();
    }

    /**
     * Write a source data record in Avro format using the given converter.
     *
     * @param record data record to write
     * @throws IOException if there is anything wrong writing the record
     */
    @Override
    public final void write(GenericRecord record) throws IOException {
        Preconditions.checkNotNull(record);

        this.writer.write(record);
        this.recordsWritten++;
    }

    /**
     * Commit the data written.
     *
     * @throws IOException if there is anything wrong committing the output
     */
    @Override
    public void commit() throws IOException {
      // Do nothing because the commit is done with Kite
    }

    /**
     * Cleanup context/resources.
     *
     * @throws IOException if there is anything wrong doing cleanup.
     */
    @Override
    public void cleanup() throws IOException {
      // Do nothing because the cleaning is done with Kite
    }

    /**
     * Get the number of records written.
     *
     * @return number of records written
     */
    @Override
    public final long recordsWritten() {
        return this.recordsWritten;
    }

    /**
     * Get the number of bytes written.
     *
     * <p>
     *     This method should ONLY be called after {@link DataWriter#commit()}
     *     is called.
     * </p>
     *
     * @return number of bytes written
     */
    @Override
    public final long bytesWritten() throws IOException {
        return this.bytesWritten;
    }

    @Override
    public final void close() throws IOException {
        if (this.writer != null) {
            this.writer.close();
        }
    }
}
