package com.adeneche;

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonObject;
import com.google.protobuf.CodedOutputStream;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;

/**
 * Parquet Metadata cache json -> proto converter
 *
 */
public class JsonToProto {

  public static class Options {
    @Option(name="-i",required = true, usage = "input filename")
    private String input;

    @Option(name="-o",required = true, usage = "output filename")
    private String output;
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

  public static void main( String[] args ) throws IOException {
    final Options options = parseArguments(args);

    final FileReader reader = new FileReader(options.input);
    final JsonObject object = Json.parse(reader).asObject();

    final FileOutputStream fileStream = new FileOutputStream(options.output);
    final CodedOutputStream codedStream = CodedOutputStream.newInstance(fileStream);

    Holders.ParquetTableMetadata tableMetadata = new JsonLoader().load(object);

    final MetadataHolder holder = new MetadataHolder();
    holder.parseFrom(tableMetadata);

    holder.writeTo(codedStream);

    fileStream.close();
    reader.close();
  }

}
