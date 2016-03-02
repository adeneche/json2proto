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
    CodedInputStream codedInputStream = CodedInputStream.newInstance(new FileInputStream(args[0]));
    codedInputStream.setSizeLimit(500_000_000);
    Metadata.ParquetTableMetadata.parseFrom(codedInputStream);

    System.out.printf("File parsed in %d ms%n", watch.elapsed(TimeUnit.MILLISECONDS));
  }
}
