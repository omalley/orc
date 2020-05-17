/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.orc.tools;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.Reader;
import org.apache.orc.StripeInformation;
import org.apache.orc.Writer;
import org.apache.orc.impl.OrcAcidUtils;

/**
 * A tool for recovering ORC files when there are missing blocks.
 */
public final class Recover {
  public static String RECOVERY_SUFFIX = ".recover";
  public static final PathFilter HIDDEN_AND_SIDE_FILE_FILTER = p -> {
    String name = p.getName();
    return !name.startsWith("_") && !name.startsWith(".") && !name.endsWith(
        OrcAcidUtils.DELTA_SIDE_FILE_SUFFIX);
  };

  // not used
  private Recover() {
  }

  public static void main(Configuration conf, String[] args) throws Exception {
    Options opts = createOptions();
    CommandLine cli = new GnuParser().parse(opts, args);

    if (cli.hasOption('h')) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("recover", opts);
      return;
    }
    boolean noFilter = cli.hasOption('a');

    String[] files = cli.getArgs();
    if (files.length == 0) {
      System.err.println("Error : ORC files are not specified");
      return;
    }

    List<LocatedFileStatus> brokenFiles = new ArrayList<>(files.length);
    for(String path: files) {
      getBrokenFiles(new Path(path), conf, noFilter, brokenFiles);
    }

    System.out.println("Broken files found: " + brokenFiles.size());
    if (brokenFiles.size() > 0) {
      String restoreDirectory;
      if (cli.hasOption("recover")) {
        restoreDirectory = cli.getOptionValue("recover");
      } else {
        restoreDirectory = "/tmp/restore-" + getTimestamp();
        Path dir = new Path(restoreDirectory);
        dir.getFileSystem(conf).mkdirs(dir);
      }
      System.out.println("Restore directory " + restoreDirectory);

      for (LocatedFileStatus file : brokenFiles) {
        recoverFile(file, conf, restoreDirectory);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    main(conf, args);
  }

  static boolean hasMissingBlocks(LocatedFileStatus status) throws IOException {
    for (BlockLocation block: status.getBlockLocations()) {
      if (block.getHosts().length == 0) {
        System.out.println(status.getPath() + " is missing blocks");
        return true;
      }
    }
    System.out.println(status.getPath() + " is fine.");
    return false;
  }

  public static void getBrokenFiles(Path path,
                                    Configuration conf,
                                    boolean noFilter,
                                    List<LocatedFileStatus> result) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    RemoteIterator<LocatedFileStatus> iterator = fs.listLocatedStatus(path);
    while (iterator.hasNext()) {
      LocatedFileStatus status = iterator.next();
      if (status.isDirectory()) {
        getBrokenFiles(status.getPath(), conf, noFilter, result);
      } else if (status.isFile() &&
          hasMissingBlocks(status) &&
          (noFilter || HIDDEN_AND_SIDE_FILE_FILTER.accept(status.getPath()))) {
        result.add(status);
      }
    }
  }

  private static void recoverFile(LocatedFileStatus corrupt,
                                  Configuration conf,
                                  String restoreDirectory) throws IOException {
    FileSystem fs = corrupt.getPath().getFileSystem(conf);
    // first recover the file to .recovered file and then once successful rename it to actual file
    Path recoveredPath = getRecoveryFile(restoreDirectory, corrupt.getPath());

    BlockLocation[] blocks = corrupt.getBlockLocations();
    if (blocks[blocks.length - 1].getHosts().length == 0) {
      System.out.println(corrupt.getPath() + " missing tail, skipping");
    } else {
      try (Reader reader = OrcFile.createReader(corrupt.getPath(), OrcFile.readerOptions(conf).filesystem(fs))) {
        recoverPieces(corrupt.getPath(), reader, blocks, conf, fs, recoveredPath);
      } catch (Exception ex) {
        System.out.println(corrupt.getPath() + " can't open tail, skipping");
      }
    }
  }

  static boolean haveData(long start, long end, BlockLocation[] blocks) throws IOException {
    for(BlockLocation block: blocks) {
      if (block.getHosts().length == 0) {
        long blockStart = block.getOffset();
        long blockEnd = blockStart + block.getLength();
        if ((blockStart <= start && start < blockEnd) ||
            (start <= blockStart && blockStart < end)) {
          return false;
        }
      }
    }
    return true;
  }

  static void recoverPieces(Path corruptPath,
                            Reader reader, BlockLocation[] blocks,
                            Configuration conf, FileSystem fs,
                            Path recoveredPath) throws IOException {
    long goodRows = 0;
    long goodStripes = 0;
    OrcFile.WriterOptions options = OrcFile.writerOptions(conf)
        .fileSystem(fs)
        .setSchema(reader.getSchema())
        .compress(reader.getCompressionKind())
        .rowIndexStride(reader.getRowIndexStride())
        .version(reader.getFileVersion())
        .setProlepticGregorian(reader.writerUsedProlepticGregorian())
        .blockSize(blocks[0].getLength())
        .overwrite(true);
    if (reader.getCompressionKind() != CompressionKind.NONE) {
      options.enforceBufferSize().bufferSize(reader.getCompressionSize());
    }
    List<OrcProto.StripeStatistics> statList = reader.getOrcProtoStripeStatistics();
    int stripeNumber = 0;
    try (Writer recover = OrcFile.createWriter(recoveredPath,options)) {
      for (StripeInformation stripe: reader.getStripes()) {
        OrcProto.StripeStatistics stats = statList.get(stripeNumber++);
        if (haveData(stripe.getOffset(), stripe.getOffset() + stripe.getLength(), blocks) &&
            stripe.getLength() < Integer.MAX_VALUE) {
          try (FSDataInputStream stream = fs.open(corruptPath)) {
            byte[] buffer = new byte[(int) stripe.getLength()];
            stream.readFully(stripe.getOffset(), buffer);
            recover.appendStripe(buffer, 0, buffer.length, stripe, stats);
            goodRows += stripe.getNumberOfRows();
            goodStripes += 1;
          } catch (IOException ioe) {
            System.out.println(corruptPath + " failed to read stripe at " +
                stripe.getOffset() + ". Ignored.");
          }
        }
      }
      System.out.println(String.format("%s recovered into %s with %d/%d stripes and %d/%d rows",
          corruptPath, recoveredPath,
          goodStripes, reader.getStripes().size(),
          goodRows, reader.getNumberOfRows()));
      // copy the user metadata
      for(String key: reader.getMetadataKeys()) {
        recover.addUserMetadata(key, reader.getMetadataValue(key));
      }
    }
  }

  static Path getRecoveryFile(String recoveryDirectory, Path corruptPath) {
    if (recoveryDirectory.equals(".")) {
      return new Path(corruptPath.toString() + RECOVERY_SUFFIX);
    } else {
      return new Path(recoveryDirectory, corruptPath.getName() + RECOVERY_SUFFIX);
    }
  }

  static String getTimestamp() {
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
    return format.format(new Date());
  }

  @SuppressWarnings("static-access")
  static Options createOptions() {
    Options result = new Options();

    result.addOption(OptionBuilder
        .withLongOpt("help")
        .withDescription("print help message")
        .create('h'));

    result.addOption(OptionBuilder
        .withLongOpt("recover")
        .withDescription("specify a recovery directory to store the recovered files (default: /tmp)")
        .hasArg()
        .create('r'));

    result.addOption(OptionBuilder
        .withLongOpt("all")
        .withDescription("don't ignore file names that start with underscore")
        .create('a'));
    return result;
  }

}
