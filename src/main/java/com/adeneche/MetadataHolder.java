package com.adeneche;

import com.adeneche.metadata.Metadata;
import com.google.common.collect.Lists;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

class MetadataHolder {
  private Metadata.MetadataHeader header;
  private Metadata.MetadataColumns columns;
  private List<Metadata.ParquetFileMetadata> files;

  @Override
  public boolean equals(Object obj) {
    if (obj == null) return false;
    if (obj == this) return true;
    MetadataHolder holder = (MetadataHolder) obj;
    return Objects.equals(header, holder.header) &&
      Objects.equals(columns, holder.columns) &&
      Objects.equals(files, holder.files);
  }

  public void parseFrom(CodedInputStream codedStream) throws IOException {
    // read header
    int length = codedStream.readRawVarint32();
    int limit = codedStream.pushLimit(length);
    header = Metadata.MetadataHeader.parseFrom(codedStream);
    codedStream.popLimit(limit);
    codedStream.resetSizeCounter();

    // read columns
    length = codedStream.readRawVarint32();
    limit = codedStream.pushLimit(length);
    columns = Metadata.MetadataColumns.parseFrom(codedStream);
    codedStream.popLimit(limit);
    codedStream.resetSizeCounter();

    // read files
    int numFiles = codedStream.readRawVarint32();
    files = Lists.newArrayList();
    for (int i = 0; i < numFiles; i++) {
      length = codedStream.readRawVarint32();
      limit = codedStream.pushLimit(length);
      files.add(Metadata.ParquetFileMetadata.parseFrom(codedStream));
      codedStream.popLimit(limit);
      codedStream.resetSizeCounter();
    }
  }

  public void writeTo(CodedOutputStream codedStream) throws IOException {
    codedStream.writeRawVarint32(header.getSerializedSize());
    header.writeTo(codedStream);

    codedStream.writeRawVarint32(columns.getSerializedSize());
    columns.writeTo(codedStream);

    codedStream.writeRawVarint32(files.size());
    for (final Metadata.ParquetFileMetadata file : files) {
      codedStream.writeRawVarint32(file.getSerializedSize());
      file.writeTo(codedStream);
    }

    codedStream.flush();
  }

  public void parseFrom(String path) throws IOException {
    CodedInputStream codedStream = CodedInputStream.newInstance(new FileInputStream(path));
    codedStream.setSizeLimit(200_000_000);

    parseFrom(codedStream);
  }

  public void parseFrom(Holders.ParquetTableMetadata tableMetadata) {
    final ProtoBuilder protoBuilder = new ProtoBuilder(tableMetadata);

    header = protoBuilder.buildHeader("v2");
    columns = protoBuilder.buildColumns();
    files = protoBuilder.buildFiles();
  }

  @Override
  public String toString() {
    return "" + header + "\n" + columns + "\n" + files;
  }
}
