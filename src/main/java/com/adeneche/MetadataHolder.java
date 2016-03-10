package com.adeneche;

import com.adeneche.metadata.Metadata;
import com.adeneche.metadata.Metadata.MetadataColumns.ColumnTypeInfo;
import com.adeneche.metadata.Metadata.ParquetFileMetadata.RowGroup;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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

  public Holders.ParquetTableMetadata toParquetTableMetadata() {
    final Holders.ParquetTableMetadata tableMetadata = new Holders.ParquetTableMetadata();

    tableMetadata.columnTypeInfo = Maps.newHashMap();
    for (final ColumnTypeInfo column : columns.getColumnsList()) {
      final Holders.ColumnTypeMetadata columnMetadata = new Holders.ColumnTypeMetadata();
      columnMetadata.name = column.getName().split(".");
      columnMetadata.primitiveType = column.getPrimitiveType();
      columnMetadata.originalType = column.getOriginalType();
      tableMetadata.columnTypeInfo.put(columnMetadata.key(), columnMetadata);
    }

    tableMetadata.files = Lists.newArrayList();
    for (final Metadata.ParquetFileMetadata file : files) {
      final Holders.ParquetFileMetadata fileMetadata = new Holders.ParquetFileMetadata();
      fileMetadata.path = file.getPath();
      fileMetadata.length = file.getLength();
      fileMetadata.rowGroups = Lists.newArrayList();
      for (final RowGroup rowGroup : file.getRowGroupsList()) {
        final Holders.RowGroupMetadata rowGroupMetadata = new Holders.RowGroupMetadata();
        rowGroupMetadata.start = rowGroup.getStart();
        rowGroupMetadata.length = rowGroup.getLength();
        rowGroupMetadata.rowCount = rowGroup.getRowCount();

        rowGroupMetadata.hostAffinity = Maps.newHashMap();
        for (final RowGroup.HostAffinity affinity : rowGroup.getAffinitiesList()) {
          rowGroupMetadata.hostAffinity.put(affinity.getKey(), affinity.getValue());
        }

        rowGroupMetadata.columns = Lists.newArrayList();
        for (final RowGroup.ColumnMetadata rowGroupColumn : rowGroup.getColumnsList()) {
          final Metadata.MetadataColumns.ColumnTypeInfo colMeta = columns.getColumns(rowGroupColumn.getName());
          final Holders.ColumnMetadata columnMetadata = new Holders.ColumnMetadata(
            colMeta.getName().split("."),
            colMeta.getPrimitiveType(),
            getMxValue(rowGroupColumn),
            rowGroupColumn.getNulls()
          );
          rowGroupMetadata.columns.add(columnMetadata);
        }
      }
    }

    tableMetadata.directories = header.getDirectoriesList();

    return tableMetadata;
  }

  private static Object getMxValue(final RowGroup.ColumnMetadata columnMetadata) {
    if (columnMetadata.hasVbinary()) return columnMetadata.getVbinary();
    else if (columnMetadata.hasVbool()) return columnMetadata.getVbool();
    else if (columnMetadata.hasVdouble()) return columnMetadata.getVdouble();
    else if (columnMetadata.hasVfloat()) return columnMetadata.getVfloat();
    else if (columnMetadata.hasVint32()) return columnMetadata.getVint32();
    else if (columnMetadata.hasVint64()) return columnMetadata.getVint64();
    return null;
  }

  @Override
  public String toString() {
    return "" + header + "\n" + columns + "\n" + files;
  }
}
