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

import org.apache.avro.generic.GenericRecord;

import gobblin.writer.partitioner.TimeBasedAvroWriterPartitioner;
import gobblin.configuration.State;

public class PNDATimeBasedAvroWriterPartitioner extends TimeBasedAvroWriterPartitioner {

  private final String prefix;

  public PNDATimeBasedAvroWriterPartitioner(State state) {
    this(state, 1, 0);
  }

  public PNDATimeBasedAvroWriterPartitioner(State state, int numBranches, int branchId) {
    super(state, numBranches, branchId);
    this.prefix = state.getPropAsBoolean(PNDAAbstractConverter.SOURCE_PROPERTY) ? "source=" : "topic=";
  }

  public GenericRecord partitionForRecord(GenericRecord record) {
    GenericRecord partition = super.partitionForRecord(record);
    partition.put(PREFIX, prefix + record.get("source"));
    return partition;
  }
}
