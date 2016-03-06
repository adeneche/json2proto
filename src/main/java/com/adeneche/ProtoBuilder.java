package com.adeneche;

import com.adeneche.Holders.ColumnTypeMetadata;
import com.adeneche.metadata.Metadata.MetadataFiles.ColumnTypeInfo;
import com.adeneche.metadata.Metadata.MetadataFiles.ParquetFileMetadata.RowGroup;
import com.adeneche.metadata.Metadata;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class ProtoBuilder {
  private final Holders.ParquetTableMetadata tableMetadata;

  private static final List<String> columnNames = Lists.newArrayList();

  public ProtoBuilder(final Holders.ParquetTableMetadata tableMetadata) {
    this.tableMetadata = tableMetadata;
  }

  public Metadata.MetadataHeader buildHeader(final String version) {
    return Metadata.MetadataHeader.newBuilder()
      .setMetadataVersion(version)
      .addAllDirectories(tableMetadata.directories)
      .build();
  }

  public Metadata.MetadataFiles buildFiles() {
    final Metadata.MetadataFiles.Builder metadataFiles = Metadata.MetadataFiles.newBuilder();

    List<ColumnTypeInfo> columns = Lists.newArrayList();
    for (final ColumnTypeMetadata.Key key : tableMetadata.columnTypeInfo.keySet()) {
      final ColumnTypeInfo columnTypeInfo = buildColumnTypeInfo(tableMetadata.columnTypeInfo.get(key));
      columns.add(columnTypeInfo);
    }

    //make sure we sort keySet so we always have same order
    Collections.sort(columns, new Comparator<ColumnTypeInfo>() {
      @Override
      public int compare(ColumnTypeInfo o1, ColumnTypeInfo o2) {
        return o1.getName().compareTo(o2.getName());
      }
    });
    metadataFiles.addAllColumns(columns);

    Iterables.addAll(columnNames, Iterables.transform(columns, new Function<ColumnTypeInfo, String>() {
      @Override
      public String apply(ColumnTypeInfo input) {
        return input.getName();
      }
    }));


    for (final Holders.ParquetFileMetadata fileMetadata : tableMetadata.files) {
      metadataFiles.addFiles(buildFile(fileMetadata));
    }

    return metadataFiles.build();
  }

  private ColumnTypeInfo buildColumnTypeInfo(ColumnTypeMetadata columnTypeMetadata) {
    final ColumnTypeInfo.Builder columnTypeInfo = ColumnTypeInfo.newBuilder();

    columnTypeInfo.setName(Utils.COLUMN_NAME_JOINER.join(columnTypeMetadata.getName()));

    if (columnTypeMetadata.primitiveType != null) {
      columnTypeInfo.setPrimitiveType(columnTypeMetadata.primitiveType);
    }

    if (columnTypeMetadata.originalType != null) {
      columnTypeInfo.setOriginalType(columnTypeMetadata.originalType);
    }

    return columnTypeInfo.build();
  }

  private Metadata.MetadataFiles.ParquetFileMetadata buildFile(Holders.ParquetFileMetadata fileMetadata) {
    final Metadata.MetadataFiles.ParquetFileMetadata.Builder file = Metadata.MetadataFiles.ParquetFileMetadata.newBuilder();

    file.setPath(fileMetadata.path);
    file.setLength(fileMetadata.length);

    for (final Holders.RowGroupMetadata rowGroupMetadata : fileMetadata.rowGroups) {
      file.addRowGroups(buildRowGroup(rowGroupMetadata));
    }

    return file.build();
  }

  private RowGroup buildRowGroup(Holders.RowGroupMetadata rowGroupMetadata) {
    final RowGroup.Builder rowGroup = Metadata.MetadataFiles.ParquetFileMetadata.RowGroup.newBuilder();

    rowGroup.setStart(rowGroupMetadata.start);
    rowGroup.setLength(rowGroupMetadata.length);
    rowGroup.setRowCount(rowGroupMetadata.rowCount);

    final Map<String, Float> hostAffinity = rowGroupMetadata.hostAffinity;
    for (final String name : hostAffinity.keySet()) {
      rowGroup.addAffinities(Metadata.MetadataFiles.ParquetFileMetadata.RowGroup.HostAffinity.newBuilder()
          .setKey(name)
          .setValue(hostAffinity.get(name))
      );
    }

    for (final Holders.ColumnMetadata column : rowGroupMetadata.columns) {
      final RowGroup.ColumnMetadata columnMetadata = buildRowGroupColumn(column);
      if (columnMetadata != null) {
        rowGroup.addColumns(columnMetadata);
      }
    }

    return rowGroup.build();
  }

  private RowGroup.ColumnMetadata buildRowGroupColumn(Holders.ColumnMetadata column) {
    Long nulls = column.nulls;
    if (nulls != null && nulls == 0) {
      nulls = null;
    }
    final Object mxValue = column.mxValue;

    if (nulls == null && mxValue == null) {
      return null;
    }

    final RowGroup.ColumnMetadata.Builder columnMetadata = RowGroup.ColumnMetadata.newBuilder();

    final int nameId = getColumnId(column.name);
    columnMetadata.setName(nameId);

    if (nulls != null) {
      columnMetadata.setNulls(nulls);
    }

    if (mxValue != null) {
      if (mxValue instanceof Long) {
        columnMetadata.setVint64((Long) mxValue);
      } else if (mxValue instanceof Integer) {
        columnMetadata.setVint32((Integer) mxValue);
      } else if (mxValue instanceof Boolean) {
        columnMetadata.setVbool((Boolean) mxValue);
      } else if (mxValue instanceof ByteString) {
        columnMetadata.setVbinary((ByteString) mxValue);
      } else if (mxValue instanceof Float) {
        columnMetadata.setVfloat((Float) mxValue);
      } else if (mxValue instanceof Double) {
        columnMetadata.setVdouble((Double) mxValue);
      } else {
        throw new RuntimeException("Unrecognized mxValue type: " + mxValue.getClass().getSimpleName());
      }
    }

    return columnMetadata.build();
  }

  private int getColumnId(final String[] columnName) {
    final String name = Utils.COLUMN_NAME_JOINER.join(columnName);
    final int nameId = columnNames.indexOf(name);
    if (nameId == -1) {
      throw new RuntimeException("column '" + name + "' not found in columnNames");
    }
    return nameId;
  }

}
