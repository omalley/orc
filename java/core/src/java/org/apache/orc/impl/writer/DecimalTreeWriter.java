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

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.OrcProto;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.IntegerWriter;
import org.apache.orc.impl.PositionRecorder;
import org.apache.orc.impl.PositionedOutputStream;

import java.io.IOException;

public class DecimalTreeWriter extends TreeWriterBase {
  private final PositionedOutputStream valueStream;

  // These scratch buffers allow us to serialize decimals much faster.
  private final long[] scratchLongs;
  private final byte[] scratchBuffer;

  private final IntegerWriter scaleStream;
  private final boolean isDirectV2;

  public DecimalTreeWriter(int columnId,
                           TypeDescription schema,
                           WriterContext writer,
                           boolean nullable) throws IOException {
    super(columnId, schema, writer, nullable);
    this.isDirectV2 = isNewWriteFormat(writer);
    valueStream = writer.createStream(id, OrcProto.Stream.Kind.DATA);
    scratchLongs = new long[HiveDecimal.SCRATCH_LONGS_LEN];
    scratchBuffer = new byte[HiveDecimal.SCRATCH_BUFFER_LEN_TO_BYTES];
    this.scaleStream = createIntegerWriter(writer.createStream(id,
        OrcProto.Stream.Kind.SECONDARY), true, isDirectV2, writer);
  }

  @Override
  OrcProto.ColumnEncoding.Builder getEncoding() {
    OrcProto.ColumnEncoding.Builder result = super.getEncoding();
    if (isDirectV2) {
      result.setKind(OrcProto.ColumnEncoding.Kind.DIRECT_V2);
    } else {
      result.setKind(OrcProto.ColumnEncoding.Kind.DIRECT);
    }
    return result;
  }

  @Override
  public void writeBatch(ColumnVector vector, int offset,
                         int length) throws IOException {
    super.writeBatch(vector, offset, length);
    DecimalColumnVector vec = (DecimalColumnVector) vector;
    if (vector.isRepeating) {
      if (vector.noNulls || !vector.isNull[0]) {
        HiveDecimalWritable value = vec.vector[0];
        indexStatistics.updateDecimal(value);
        if (createBloomFilter) {
          String str = value.toString(scratchBuffer);
          if (bloomFilter != null) {
            bloomFilter.addString(str);
          }
          bloomFilterUtf8.addString(str);
        }
        for (int i = 0; i < length; ++i) {
          value.serializationUtilsWrite(valueStream,
              scratchLongs);
          scaleStream.write(value.scale());
        }
      }
    } else {
      for (int i = 0; i < length; ++i) {
        if (vec.noNulls || !vec.isNull[i + offset]) {
          HiveDecimalWritable value = vec.vector[i + offset];
          value.serializationUtilsWrite(valueStream, scratchLongs);
          scaleStream.write(value.scale());
          indexStatistics.updateDecimal(value);
          if (createBloomFilter) {
            String str = value.toString(scratchBuffer);
            if (bloomFilter != null) {
              bloomFilter.addString(str);
            }
            bloomFilterUtf8.addString(str);
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
    valueStream.flush();
    scaleStream.flush();
  }

  @Override
  public long estimateMemory() {
    return super.estimateMemory() + valueStream.getBufferSize() +
        scaleStream.estimateMemory();
  }

  @Override
  public long getRawDataSize() {
    return fileStatistics.getNumberOfValues() *
        JavaDataModel.get().lengthOfDecimal();
  }
}
