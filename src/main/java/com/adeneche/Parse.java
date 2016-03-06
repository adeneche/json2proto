package com.adeneche;

import com.adeneche.metadata.Metadata;
import com.google.common.base.Stopwatch;
import com.google.protobuf.CodedInputStream;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class Parse {

  private static class Options {
    @Option(name = "-i", required = true)
    private String input;
    @Option(name = "-v")
    private boolean verbose;
    @Option(name = "-s")
    private int size = -1;
  }

  private static Options parseArguments(String[] args) {
    final Options options = new Options();
    final CmdLineParser parser = new CmdLineParser(options);

    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      // if there's a problem in the command line,
      // you'll get this exception. this will report
      // an error message.
      System.err.println(e.getMessage());
      System.exit(-1);
    }

    return options;
  }

  public static void main(String[] args) throws IOException {
    final Options options = parseArguments(args);

    Stopwatch watch = Stopwatch.createStarted();
    CodedInputStream codedStream = CodedInputStream.newInstance(new FileInputStream(options.input));
    if (options.size > 0) {
      codedStream.setSizeLimit(options.size);
    }

    // read header
    int length = codedStream.readRawVarint32();
    int limit = codedStream.pushLimit(length);
    final Metadata.MetadataHeader header = Metadata.MetadataHeader.parseFrom(codedStream);
    codedStream.popLimit(limit);

    // read files
    length = codedStream.readRawVarint32();
    limit = codedStream.pushLimit(length);
    final Metadata.MetadataFiles files = Metadata.MetadataFiles.parseFrom(codedStream);
    codedStream.popLimit(limit);

    if (options.verbose) {
      System.out.printf("File parsed in %d ms%n", watch.elapsed(TimeUnit.MILLISECONDS));
    } else {
      System.out.println(header);
      System.out.println(files);
    }
  }
}
