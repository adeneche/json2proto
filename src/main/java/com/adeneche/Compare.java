package com.adeneche;

import java.io.IOException;

public class Compare {

  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      System.err.println("USAGE file1 file2");
      System.exit(-1);
    }

    MetadataHolder holder1 = new MetadataHolder();
    holder1.parseFrom(args[0]);
    MetadataHolder holder2 = new MetadataHolder();
    holder2.parseFrom(args[1]);

    if (!holder1.equals(holder2)) {
      System.err.println("Difference found :(");
    } else {
      System.out.println("no difference was found =D");
    }
  }
}
