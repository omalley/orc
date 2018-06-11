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

package org.apache.orc.bench;

import com.netflix.iceberg.DataFile;
import com.netflix.iceberg.DataFiles;
import com.netflix.iceberg.PartitionSpec;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.Table;
import com.netflix.iceberg.hadoop.HadoopTables;
import com.netflix.iceberg.spark.source.IcebergSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.TrackingLocalFileSystem;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.DataReader;
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;
import org.apache.spark.sql.sources.v2.reader.SupportsScanUnsafeRow;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.netflix.iceberg.Files.localOutput;
import static org.apache.ivy.plugins.parser.xml.XmlModuleDescriptorParser.Parser.State.CONF;

@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations=1, time=1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations=1, time=1, timeUnit = TimeUnit.SECONDS)
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
public class IcebergBenchmark {

  private static final String ROOT_ENVIRONMENT_NAME = "bench.root.dir";
  private static final Path root;
  static {
    String value = System.getProperty(ROOT_ENVIRONMENT_NAME);
    root = value == null ? null : new Path(value);
  }

  @State(Scope.Thread)
  public static class InputSource {
    SparkSession session;
    TrackingLocalFileSystem fs;
    Configuration conf;
    Path tablePath;
    HadoopTables tables;
    Table table;
    Schema schema;
    IcebergSource source;
    DataSourceOptions options;

    @Param({"taxi", "sales", "github"})
    String dataset;

    @Param({"none", "zlib", "snappy"})
    String compression;

    @Param({"orc", "parquet", "avro"})
    String format;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      session = SparkSession.builder().appName("benchmark")
          .config("spark.master", "local[4]").getOrCreate();
      conf = new Configuration();
      conf.set("fs.track.impl", TrackingLocalFileSystem.class.getName());
      FileSystem localFs = FileSystem.getLocal(conf).getRaw();
      Path tableDirectory = new Path(root, "table");
      localFs.mkdirs(tableDirectory);
      tablePath = new Path(root, "table/" + dataset + "-" + compression +
          "-" + format + ".tbl");
      localFs.delete(tablePath, true);
      tables = new HadoopTables(conf);
      Schema origSchema = Utilities.toIcebergType(Utilities.loadSchema(
          dataset + ".schema"));
      table = tables.create(origSchema, PartitionSpec.unpartitioned(),
          tablePath.toString());

      // Important: use the table's schema for the rest of the test
      // When tables are created, the column ids are reassigned.
      schema = table.schema();
      Path dataPath = new Path("track://",
          Utilities.getVariant(root, dataset, format, compression));
      fs = (TrackingLocalFileSystem) dataPath.getFileSystem(conf);

      DataFile file = DataFiles.builder(PartitionSpec.unpartitioned())
          .withFileSizeInBytes(fs.getLength(dataPath))
          .withPath(dataPath.toString())
          .withFormat(format)
          .withRecordCount(1000)
          .build();
      table.newAppend().appendFile(file).commit();
      source = new IcebergSource();
      Map<String,String> optMap = new HashMap<>();
      optMap.put("path", tablePath.toString());
      options = new DataSourceOptions(optMap);
    }
  }

  @AuxCounters(AuxCounters.Type.EVENTS)
  @State(Scope.Thread)
  public static class ExtraCounters {
    long bytesRead;
    long reads;
    long records;
    long times;

    @Setup(Level.Iteration)
    public void reset() {
      bytesRead = 0;
      reads = 0;
      records = 0;
      times = 0;
    }

    @TearDown(Level.Iteration)
    public void print() {
      System.out.println();
      System.out.println("Reads: " + reads);
      System.out.println("Bytes: " + bytesRead);
      System.out.println("Records: " + records);
      System.out.println("Invocations: " + times);
    }

    public long kilobytes() {
      return times == 0 ? 0 : bytesRead / 1024 / times;
    }

    public long records() {
      return times == 0 ? 0 : records / times;
    }
  }

  @Benchmark
  public void fullRead(InputSource source,
                       ExtraCounters counters) throws IOException {
    FileSystem.Statistics statistics = source.fs.getLocalStatistics();
    statistics.reset();
    SupportsScanUnsafeRow reader =
        (SupportsScanUnsafeRow) source.source.createReader(source.options);
    for(DataReaderFactory<UnsafeRow> factory: reader.createUnsafeRowReaderFactories()) {
      DataReader<UnsafeRow> rows = factory.createDataReader();
      while (rows.next()) {
        counters.records += 1;
      }
    }
    counters.bytesRead += statistics.getBytesRead();
    counters.reads += statistics.getReadOps();
    counters.times += 1;
  }

  public static void main(String[] args) throws Exception {
    String dataPath = new File(args[0]).getCanonicalPath();
    new Runner(new OptionsBuilder()
        .include(IcebergBenchmark.class.getSimpleName())
        // .addProfiler("hs_gc")
        .jvmArgs("-server", "-Xms256m", "-Xmx2g",
            "-D" + ROOT_ENVIRONMENT_NAME + "=" + dataPath).build()
    ).run();
  }
}
