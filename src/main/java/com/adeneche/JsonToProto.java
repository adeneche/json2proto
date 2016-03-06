package com.adeneche;

import com.adeneche.metadata.Metadata;
import com.adeneche.metadata.Metadata.MetadataFiles.ColumnTypeInfo;
import com.adeneche.metadata.Metadata.MetadataFiles.ParquetFileMetadata;
import com.adeneche.metadata.Metadata.MetadataFiles.ParquetFileMetadata.RowGroup;
import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

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

    Metadata.MetadataHeader header = extractHeader(object);
    codedStream.writeRawVarint32(header.getSerializedSize());
    header.writeTo(codedStream);

    Metadata.MetadataFiles files = extractFiles(object);
    codedStream.writeRawVarint32(files.getSerializedSize());
    files.writeTo(codedStream);

    codedStream.flush();
    fileStream.close();
    reader.close();
  }

  private static final List<String> columnNames = Lists.newArrayList();
  private static final List<ColumnTypeInfo.PrimitiveTypeName> columnTypes = Lists.newArrayList();

  private static Metadata.MetadataFiles extractFiles(JsonObject object) {
    final Metadata.MetadataFiles.Builder metadataFiles = Metadata.MetadataFiles.newBuilder();

    final JsonObject columns = object.get("columnTypeInfo").asObject();

    List<ColumnTypeInfo> columnsTypeInfos = Lists.newArrayList();
    for (final String name : columns.names()) {
      final ColumnTypeInfo columnTypeInfo = parseColumnTypeInfo(columns.get(name).asObject());
      columnsTypeInfos.add(columnTypeInfo);
    }

    //make sure we sort keySet so we always have same order
    Collections.sort(columnsTypeInfos, new Comparator<ColumnTypeInfo>() {
      @Override
      public int compare(ColumnTypeInfo o1, ColumnTypeInfo o2) {
        return o1.getName().compareTo(o2.getName());
      }
    });
    metadataFiles.addAllColumns(columnsTypeInfos);

    Iterables.addAll(columnNames, Iterables.transform(columnsTypeInfos,
      new Function<ColumnTypeInfo, String>() {
        @Override
        public String apply(ColumnTypeInfo input) {
          return input.getName();
        }
      }
    ));

    Iterables.addAll(columnTypes, Iterables.transform(columnsTypeInfos,
      new Function<ColumnTypeInfo, ColumnTypeInfo.PrimitiveTypeName>() {
        @Override
        public ColumnTypeInfo.PrimitiveTypeName apply(ColumnTypeInfo input) {
          return input.getPrimitiveType();
        }
      }
    ));

    final JsonArray files = object.get("files").asArray();
    for (int i = 0; i < files.size(); i++) {
      if (i % 1000 == 0) {
        System.out.printf("processing file %d/%d%n", i, files.size());
      }
      metadataFiles.addFiles(parseFile(files.get(i).asObject()));
    }

    return metadataFiles.build();
  }

  private static Metadata.MetadataHeader extractHeader(final JsonObject object) {
    final String version = object.get("metadata_version").asString();
    if (!"v2".equals(version)) {
      throw new RuntimeException("invalid metadata_version: " + version);
    }

    return Metadata.MetadataHeader.newBuilder()
      .setMetadataVersion(version)
      .addAllDirectories(parseStringArray(object.get("directories").asArray()))
      .build();
  }

  private static ParquetFileMetadata parseFile(final JsonObject object) {
    final ParquetFileMetadata.Builder file = ParquetFileMetadata.newBuilder();

    file.setPath(object.get("path").asString());

    file.setLength(object.get("length").asLong());

    final JsonArray rowGroups = object.get("rowGroups").asArray();
    for (int i = 0; i < rowGroups.size(); i++) {
      file.addRowGroups(parseRowGroup(rowGroups.get(i).asObject()));
    }

    return file.build();
  }

  private static RowGroup parseRowGroup(final JsonObject object) {
    final RowGroup.Builder rowGroup = RowGroup.newBuilder();

    rowGroup.setStart(object.get("start").asLong());
    rowGroup.setLength(object.get("length").asLong());
    rowGroup.setRowCount(object.get("rowCount").asLong());

    final JsonObject hostAffinity = object.get("hostAffinity").asObject();
    for (final String name : hostAffinity.names()) {
      rowGroup.addAffinities(RowGroup.HostAffinity.newBuilder()
        .setKey(name)
        .setValue(hostAffinity.get(name).asFloat())
      );
    }

    final JsonArray columns = object.get("columns").asArray();
    for (final JsonValue column : columns) {
      final RowGroup.ColumnMetadata columnMetadata = parseRowGroupColumn(column.asObject());
      if (columnMetadata != null) {
        rowGroup.addColumns(columnMetadata);
      }
    }

    return rowGroup.build();
  }

  private static RowGroup.ColumnMetadata parseRowGroupColumn(final JsonObject object) {
    final long nulls = object.get("nulls").asLong();
    final JsonValue mxValue = object.get("mxValue");

    if (nulls == 0 && mxValue == null) {
      return null;
    }

    final RowGroup.ColumnMetadata.Builder column = RowGroup.ColumnMetadata.newBuilder();

    final String name = Joiner.on(".").join(parseStringArray(object.get("name").asArray()));
    final int nameId = columnNames.indexOf(name);
    if (nameId == -1) {
      System.err.println("column '" + name + "' not found in columnNames");
      System.exit(-1);
    }
    column.setName(nameId);

    if (nulls > 0) {
      column.setNulls(nulls);
    }

    if (mxValue != null) {
      final ColumnTypeInfo.PrimitiveTypeName type = columnTypes.get(nameId);
      switch (type) {
        case INT64:
          column.setVint64(mxValue.asLong());
          break;
        case INT32:
          column.setVint32(mxValue.asInt());
          break;
        case BOOLEAN:
          column.setVbool(mxValue.asBoolean());
          break;
        case BINARY:
        case FIXED_LEN_BYTE_ARRAY:
        case INT96:
          column.setVbinary(ByteString.copyFrom(mxValue.asString().getBytes()));
          break;
        case FLOAT:
          column.setVfloat(mxValue.asFloat());
          break;
        case DOUBLE:
          column.setVdouble(mxValue.asDouble());
          break;
      }
    }

    return column.build();
  }

  private static ColumnTypeInfo parseColumnTypeInfo(JsonObject object) {
    final ColumnTypeInfo.Builder columnTypeInfo = ColumnTypeInfo.newBuilder();

    columnTypeInfo.setName(Joiner.on(".").join(parseStringArray(object.get("name").asArray())));

    final JsonValue primitiveType = object.get("primitiveType");
    if (!primitiveType.isNull()) {
      columnTypeInfo.setPrimitiveType(ColumnTypeInfo.PrimitiveTypeName.valueOf(primitiveType.asString()));
    }

    final JsonValue originalType = object.get("originalType");
    if (!originalType.isNull()) {
      columnTypeInfo.setOriginalType(ColumnTypeInfo.OriginalType.valueOf(originalType.asString()));
    }

    return columnTypeInfo.build();
  }

  private static List<String> parseStringArray(final JsonArray array) {
    final List<String> strings = Lists.newArrayList();
    for (int i = 0; i < array.size(); i++) {
      strings.add(array.get(i).asString());
    }
    return strings;
  }
}
