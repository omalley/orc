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

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class StringTreeWriter extends StringBaseTreeWriter {
  StringTreeWriter(int columnId,
                   TypeDescription schema,
                   WriterContext writer,
                   boolean nullable) throws IOException {
    super(columnId, schema, writer, nullable);
  }

  @Override
  public void writeBatch(ColumnVector vector, int offset,
                         int length) throws IOException {
    super.writeBatch(vector, offset, length);
    BytesColumnVector vec = (BytesColumnVector) vector;
    if (vector.isRepeating) {
      if (vector.noNulls || !vector.isNull[0]) {
        if (doneDictionaryCheck) {
          if (useDictionaryEncoding) {
            int entry = dictionary.add(vec.vector[0],
                vec.start[0], vec.length[0]);
            for(int r=0; r < length; ++r) {
              rowOutput.write(entry);
            }
          } else {
            for(int r=0; r < length; ++r) {
              directStreamOutput.write(vec.vector[0],
                  vec.start[0], vec.length[0]);
              directLengthOutput.write(vec.length[0]);
            }
          }
        } else {
          int entry = dictionary.add(vec.vector[0],
              vec.start[0], vec.length[0]);
          for(int r=0; r < length; ++r) {
            rows.add(entry);
          }
        }
        indexStatistics.updateString(vec.vector[0], vec.start[0],
            vec.length[0], length);
        if (createBloomFilter) {
          if (bloomFilter != null) {
            // translate from UTF-8 to the default charset
            bloomFilter.addString(new String(vec.vector[0], vec.start[0],
                vec.length[0], StandardCharsets.UTF_8));
          }
          bloomFilterUtf8.addBytes(vec.vector[0], vec.start[0], vec.length[0]);
        }
      }
    } else {
      for (int i = 0; i < length; ++i) {
        if (vec.noNulls || !vec.isNull[i + offset]) {
          if (doneDictionaryCheck) {
            if (useDictionaryEncoding) {
              rowOutput.write(dictionary.add(vec.vector[offset + i],
                  vec.start[offset + i], vec.length[offset + i]));
            } else {
              directStreamOutput.write(vec.vector[offset + i],
                  vec.start[offset + i], vec.length[offset + i]);
              directLengthOutput.write(vec.length[offset + i]);
            }
          } else {
            rows.add(dictionary.add(vec.vector[offset + i],
                vec.start[offset + i], vec.length[offset + i]));
          }
          indexStatistics.updateString(vec.vector[offset + i],
              vec.start[offset + i], vec.length[offset + i], 1);
          if (createBloomFilter) {
            if (bloomFilter != null) {
              // translate from UTF-8 to the default charset
              bloomFilter.addString(new String(vec.vector[offset + i],
                  vec.start[offset + i], vec.length[offset + i],
                  StandardCharsets.UTF_8));
            }
            bloomFilterUtf8.addBytes(vec.vector[offset + i],
                vec.start[offset + i], vec.length[offset + i]);
          }
        }
      }
    }
  }
}
