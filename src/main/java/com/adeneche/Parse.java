package com.adeneche;

import com.adeneche.metadata.Metadata;
import com.google.common.base.Stopwatch;
import com.google.protobuf.CodedInputStream;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class Parse {
  public static void main(String[] args) throws IOException {
    if (args.length != 1) {
      System.err.println("USAGE: Parse filename. args.length="+args.length);
      System.exit(-1);
    }

    Stopwatch watch = Stopwatch.createStarted();
    CodedInputStream codedStream = CodedInputStream.newInstance(new FileInputStream(args[0]));

    // read header
    int length = codedStream.readRawVarint32();
    int limit = codedStream.pushLimit(length);
    Metadata.MetadataHeader.parseFrom(codedStream);
    codedStream.popLimit(limit);

    // read files
    length = codedStream.readRawVarint32();
    limit = codedStream.pushLimit(length);
    Metadata.MetadataFiles.parseFrom(codedStream);
    codedStream.popLimit(limit);

    System.out.printf("File parsed in %d ms%n", watch.elapsed(TimeUnit.MILLISECONDS));
  }
}
