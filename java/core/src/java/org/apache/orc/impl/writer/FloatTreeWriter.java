/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc.impl.writer;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.orc.OrcProto;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.PositionRecorder;
import org.apache.orc.impl.PositionedOutputStream;
import org.apache.orc.impl.SerializationUtils;

import java.io.IOException;

public class FloatTreeWriter extends TreeWriterBase {
  private final PositionedOutputStream stream;
  private final SerializationUtils utils;

  public FloatTreeWriter(int columnId,
                         TypeDescription schema,
                         WriterContext writer,
                         boolean nullable) throws IOException {
    super(columnId, schema, writer, nullable);
    this.stream = writer.createStream(id,
        OrcProto.Stream.Kind.DATA);
    this.utils = new SerializationUtils();
  }

  @Override
  public void writeBatch(ColumnVector vector, int offset,
                         int length) throws IOException {
    super.writeBatch(vector, offset, length);
    DoubleColumnVector vec = (DoubleColumnVector) vector;
    if (vector.isRepeating) {
      if (vector.noNulls || !vector.isNull[0]) {
        float value = (float) vec.vector[0];
        indexStatistics.updateDouble(value);
        if (createBloomFilter) {
          if (bloomFilter != null) {
            bloomFilter.addDouble(value);
          }
          bloomFilterUtf8.addDouble(value);
        }
        for (int i = 0; i < length; ++i) {
          utils.writeFloat(stream, value);
        }
      }
    } else {
      for (int i = 0; i < length; ++i) {
        if (vec.noNulls || !vec.isNull[i + offset]) {
          float value = (float) vec.vector[i + offset];
          utils.writeFloat(stream, value);
          indexStatistics.updateDouble(value);
          if (createBloomFilter) {
            if (bloomFilter != null) {
              bloomFilter.addDouble(value);
            }
            bloomFilterUtf8.addDouble(value);
          }
        }
      }
    }
  }


  @Override
  public void writeStripe(OrcProto.StripeFooter.Builder builder,
                          OrcProto.StripeStatistics.Builder stats,
                          int requiredIndexEntries) throws IOException {
    super.writeStripe(builder, stats, requiredIndexEntries);
    stream.flush();
  }

  @Override
  public long estimateMemory() {
    return super.estimateMemory() + stream.getBufferSize();
  }

  @Override
  public long getRawDataSize() {
    long num = fileStatistics.getNumberOfValues();
    return num * JavaDataModel.get().primitive1();
  }
}
