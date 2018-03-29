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
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcProto;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.util.List;

public class StructTreeWriter extends TreeWriterBase {
  final TreeWriter[] childrenWriters;

  public StructTreeWriter(int columnId,
                          TypeDescription schema,
                          WriterContext writer,
                          boolean nullable) throws IOException {
    super(columnId, schema, writer, nullable);
    List<TypeDescription> children = schema.getChildren();
    childrenWriters = new TreeWriterBase[children.size()];
    for (int i = 0; i < childrenWriters.length; ++i) {
      childrenWriters[i] = Factory.create(children.get(i), writer, true);
    }
  }

  @Override
  public void writeRootBatch(VectorizedRowBatch batch, int offset,
                             int length) throws IOException {
    // update the statistics for the root column
    indexStatistics.increment(length);
    // I'm assuming that the root column isn't nullable so that I don't need
    // to update isPresent.
    for (int i = 0; i < childrenWriters.length; ++i) {
      childrenWriters[i].writeBatch(batch.cols[i], offset, length);
    }
  }

  private static void writeFields(StructColumnVector vector,
                                  TreeWriter[] childrenWriters,
                                  int offset, int length) throws IOException {
    for (int field = 0; field < childrenWriters.length; ++field) {
      childrenWriters[field].writeBatch(vector.fields[field], offset, length);
    }
  }

  @Override
  public void writeBatch(ColumnVector vector, int offset,
                         int length) throws IOException {
    super.writeBatch(vector, offset, length);
    StructColumnVector vec = (StructColumnVector) vector;
    if (vector.isRepeating) {
      if (vector.noNulls || !vector.isNull[0]) {
        writeFields(vec, childrenWriters, offset, length);
      }
    } else if (vector.noNulls) {
      writeFields(vec, childrenWriters, offset, length);
    } else {
      // write the records in runs
      int currentRun = 0;
      boolean started = false;
      for (int i = 0; i < length; ++i) {
        if (!vec.isNull[i + offset]) {
          if (!started) {
            started = true;
            currentRun = i;
          }
        } else if (started) {
          started = false;
          writeFields(vec, childrenWriters, offset + currentRun,
              i - currentRun);
        }
      }
      if (started) {
        writeFields(vec, childrenWriters, offset + currentRun,
            length - currentRun);
      }
    }
  }

  @Override
  public void createRowIndexEntry() throws IOException {
    super.createRowIndexEntry();
    for (TreeWriter child : childrenWriters) {
      child.createRowIndexEntry();
    }
  }

  @Override
  public void writeStripe(OrcProto.StripeFooter.Builder builder,
                          OrcProto.StripeStatistics.Builder stats,
                          int requiredIndexEntries) throws IOException {
    super.writeStripe(builder, stats, requiredIndexEntries);
    for (TreeWriter child : childrenWriters) {
      child.writeStripe(builder, stats, requiredIndexEntries);
    }
  }

  @Override
  public void updateFileStatistics(OrcProto.StripeStatistics stats) {
    super.updateFileStatistics(stats);
    for (TreeWriter child : childrenWriters) {
      child.updateFileStatistics(stats);
    }
  }

  @Override
  public long estimateMemory() {
    long result = 0;
    for (TreeWriter writer : childrenWriters) {
      result += writer.estimateMemory();
    }
    return super.estimateMemory() + result;
  }

  @Override
  public long getRawDataSize() {
    long result = 0;
    for (TreeWriter writer : childrenWriters) {
      result += writer.getRawDataSize();
    }
    return result;
  }

  @Override
  public void writeFileStatistics(OrcProto.Footer.Builder footer) {
    super.writeFileStatistics(footer);
    for (TreeWriter child : childrenWriters) {
      child.writeFileStatistics(footer);
    }
  }
}
