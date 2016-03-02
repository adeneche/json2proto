package com.adeneche;

import com.adeneche.metadata.Metadata;
import com.google.common.base.Stopwatch;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class Parse {
  public static void main(String[] args) throws IOException {
    if (args.length != -1) {
      System.err.println("USAGE: Parse filename");
      System.exit(-1);
    }

    Stopwatch watch = Stopwatch.createStarted();
    Metadata.ParquetTableMetadata.parseFrom(new FileInputStream(args[0]));
    System.out.printf("File parsed in %d ms%n", watch.elapsed(TimeUnit.MILLISECONDS));
  }
}
