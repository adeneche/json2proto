package com.adeneche;

import com.adeneche.test.PObject;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class MultiPart {
  private static class Options {
    @Option(name = "-r", usage = "read mode")
    private boolean read;

    @Option(name="-f",required = true, usage = "filename")
    private String file;

    @Option(name = "-n", usage = "num bodies")
    private long numBodies = 100;
  }

  private static Options parseArguments(String[] args) {
    final Options options = new Options();
    final CmdLineParser parser = new CmdLineParser(options);

    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      System.exit(-1);
    }

    return options;
  }

  public static void main(String[] args) throws IOException {
    final Options options = parseArguments(args);

    if (options.read) {
      System.out.println("Reading " + options.file);
      read(options.file);
    } else {
      System.out.println("Writing " + options.file);
      write(options.file, options.numBodies);
    }
  }

  private static void read(String filename) throws IOException {
    final InputStream fileStream = new FileInputStream(filename);
    final CodedInputStream codedStream = CodedInputStream.newInstance(fileStream);

    int length = codedStream.readRawVarint32();
    int limit = codedStream.pushLimit(length);
    PObject.Header header = PObject.Header.parseFrom(codedStream);
    codedStream.popLimit(limit);
    System.out.printf("version: %s%nnumBodies: %d%n", header.getVersion(), header.getNumBodies());

    final long numBodies = header.getNumBodies();
    for (long i = 0; i < numBodies; i++) {
      length = codedStream.readRawVarint32();
      limit = codedStream.pushLimit(length);
      PObject.BodyPart part = PObject.BodyPart.parseFrom(codedStream);
      codedStream.popLimit(limit);
      System.out.printf("part[id= %d, data= '%s']%n", part.getId(), part.getData());
    }

    fileStream.close();
  }

  private static void write(String filename, long numBodies) throws IOException {
    final OutputStream fileStream = new FileOutputStream(filename);
    final CodedOutputStream codedStream = CodedOutputStream.newInstance(fileStream);

    PObject.Header header = PObject.Header.newBuilder()
      .setVersion("v1.0")
      .setNumBodies(numBodies)
      .build();
    codedStream.writeRawVarint32(header.getSerializedSize());
    header.writeTo(codedStream);

    for (long i = 0; i < numBodies; i++) {
      final PObject.BodyPart part = PObject.BodyPart.newBuilder()
        .setId(i)
        .setData("body part #" + i)
        .build();
      codedStream.writeRawVarint32(part.getSerializedSize());
      part.writeTo(codedStream);
    }

    codedStream.flush();
    fileStream.close();
  }
}
