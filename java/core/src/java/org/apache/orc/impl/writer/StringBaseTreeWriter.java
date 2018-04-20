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

import com.jcraft.jsch.IO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.io.Text;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcProto;
import org.apache.orc.StringColumnStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.DynamicIntArray;
import org.apache.orc.impl.IntegerWriter;
import org.apache.orc.impl.OutStream;
import org.apache.orc.impl.PositionRecorder;
import org.apache.orc.impl.PositionedOutputStream;
import org.apache.orc.impl.StringRedBlackTree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class StringBaseTreeWriter extends TreeWriterBase {
  private static final int INITIAL_DICTIONARY_SIZE = 4096;
  private final OutStream stringOutput;
  protected final IntegerWriter lengthOutput;
  protected final IntegerWriter rowOutput;
  protected final StringRedBlackTree dictionary =
      new StringRedBlackTree(INITIAL_DICTIONARY_SIZE);
  protected final DynamicIntArray rows = new DynamicIntArray();
  protected final OutStream directStreamOutput;
  protected final IntegerWriter directLengthOutput;
  // If the number of keys in a dictionary is greater than this fraction of
  //the total number of non-null rows, turn off dictionary encoding
  private final double dictionaryKeySizeThreshold;
  protected boolean useDictionaryEncoding = true;
  private boolean isDirectV2;
  protected boolean doneDictionaryCheck;

  StringBaseTreeWriter(int columnId,
                       TypeDescription schema,
                       WriterContext writer,
                       boolean nullable) throws IOException {
    super(columnId, schema, writer, nullable);
    this.isDirectV2 = isNewWriteFormat(writer);
    directStreamOutput = writer.createStream(id, OrcProto.Stream.Kind.DATA);
    directLengthOutput = createIntegerWriter(writer.createStream(id,
        OrcProto.Stream.Kind.LENGTH), false, isDirectV2, writer);
    stringOutput = writer.createStream(id,
        OrcProto.Stream.Kind.DICTIONARY_DATA);
    lengthOutput = createIntegerWriter(writer.createStream(id,
        OrcProto.Stream.Kind.DICTIONARY_LENGTH), false, isDirectV2, writer);
    rowOutput = createIntegerWriter(directStreamOutput, false, isDirectV2,
        writer);
    Configuration conf = writer.getConfiguration();
    dictionaryKeySizeThreshold =
        OrcConf.DICTIONARY_KEY_SIZE_THRESHOLD.getDouble(conf);
    doneDictionaryCheck = false;
  }

  private void checkDictionaryEncoding() throws IOException {
    if (!doneDictionaryCheck) {
      // Set the flag indicating whether or not to use dictionary encoding
      // based on whether or not the fraction of distinct keys over number of
      // non-null rows is less than the configured threshold
      float ratio = rows.size() > 0 ? (float) (dictionary.size()) / rows.size() : 0.0f;
      useDictionaryEncoding = !isDirectV2 || ratio <= dictionaryKeySizeThreshold;
      doneDictionaryCheck = true;
      if (useDictionaryEncoding) {
        writeDictionaryRows();
      } else {
        convertToDirect();
      }
    }
  }

  /**
   * Write the buffered rows to the stream once the writer has committed to
   * using a dictionary for this column.
   */
  private void writeDictionaryRows() throws IOException {
    for(int r=0; r < rows.size(); ++r) {
      rowOutput.write(rows.get(r));
    }
    rows.clear();
  }

  /**
   * Switch to a direct encoding.
   */
  private void convertToDirect() throws IOException {
    Text value = new Text();
    for(int r=0; r < rows.size(); ++r) {
      int entry = rows.get(r);
      dictionary.getText(value, entry);
      directStreamOutput.write(value.getBytes(), 0, value.getLength());
      directLengthOutput.write(value.getLength());
    }
    dictionary.clear();
    rows.clear();
  }

  @Override
  public void flushStreams() throws IOException {
    checkDictionaryEncoding();
    super.flushStreams();
    if (useDictionaryEncoding) {
      rowOutput.flush();
      directLengthOutput.suppress();
    } else {
      directLengthOutput.flush();
      directStreamOutput.flush();
      stringOutput.suppress();
      lengthOutput.suppress();
    }
  }

  @Override
  public void writeStripe(OrcProto.StripeFooter.Builder builder,
                          OrcProto.StripeStatistics.Builder stats,
                          int requiredIndexEntries) throws IOException {
    super.writeStripe(builder, stats, requiredIndexEntries);

    if (useDictionaryEncoding) {
      writeDictionary();
    }
  }

  private void writeDictionary() throws IOException {
    Text value = new Text();
    for(int e=0; e < dictionary.size(); ++e) {
      dictionary.getText(value, e);
      stringOutput.write(value.getBytes(), 0, value.getLength());
      lengthOutput.write(value.getLength());
    }
    stringOutput.flush();
    lengthOutput.flush();
    dictionary.clear();
  }

  @Override
  OrcProto.ColumnEncoding.Builder getEncoding() {
    OrcProto.ColumnEncoding.Builder result = super.getEncoding();
    if (useDictionaryEncoding) {
      result.setDictionarySize(dictionary.size());
      if (isDirectV2) {
        result.setKind(OrcProto.ColumnEncoding.Kind.DICTIONARY_V2);
      } else {
        result.setKind(OrcProto.ColumnEncoding.Kind.DICTIONARY);
      }
    } else {
      if (isDirectV2) {
        result.setKind(OrcProto.ColumnEncoding.Kind.DIRECT_V2);
      } else {
        result.setKind(OrcProto.ColumnEncoding.Kind.DIRECT);
      }
    }
    return result;
  }

  @Override
  public long estimateMemory() {
    long parent = super.estimateMemory();
    if (useDictionaryEncoding) {
      return parent + dictionary.getSizeInBytes() + rows.getSizeInBytes();
    } else {
      return parent + lengthOutput.estimateMemory() +
          directStreamOutput.getBufferSize();
    }
  }

  @Override
  public long getRawDataSize() {
    // ORC strings are converted to java Strings. so use JavaDataModel to
    // compute the overall size of strings
    StringColumnStatistics scs = (StringColumnStatistics) fileStatistics;
    long numVals = fileStatistics.getNumberOfValues();
    if (numVals == 0) {
      return 0;
    } else {
      int avgSize = (int) (scs.getSum() / numVals);
      return numVals * JavaDataModel.get().lengthForStringOfLength(avgSize);
    }
  }
}
