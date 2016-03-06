package com.adeneche;

import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import com.google.common.collect.Lists;
import com.adeneche.metadata.Metadata.MetadataFiles.ColumnTypeInfo.OriginalType;
import com.adeneche.metadata.Metadata.MetadataFiles.ColumnTypeInfo.PrimitiveTypeName;
import com.adeneche.Holders.ColumnTypeMetadata;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.adeneche.Utils.parseStringArray;

/**
 * loads a .json metadata cache file into Holders
 */
public class JsonLoader {

  private Holders.ParquetTableMetadata tableMetadata;

  private final List<String> columnNames = Lists.newArrayList();
  private final List<PrimitiveTypeName> columnTypes = Lists.newArrayList();

  public JsonLoader() {
    tableMetadata = new Holders.ParquetTableMetadata();
  }

  public Holders.ParquetTableMetadata load(final JsonObject obj) {
    final String version = obj.get("metadata_version").asString();
    if (!"v2".equals(version)) {
      throw new RuntimeException("invalid metadata_version: " + version);
    }

    extractRootColumns(obj);
    extractFiles(obj);
    tableMetadata.directories = Arrays.asList(parseStringArray(obj.get("directories").asArray()));

    return tableMetadata;
  }

  private void extractRootColumns(final JsonObject obj) {
    tableMetadata.columnTypeInfo = Maps.newHashMap();

    final JsonObject columns = obj.get("columnTypeInfo").asObject();
    for (final String name : columns.names()) {
      final Holders.ColumnTypeMetadata columnTypeMetadata = parseColumnTypeInfo(columns.get(name).asObject());
      columnNames.add(Utils.COLUMN_NAME_JOINER.join(columnTypeMetadata.getName()));
      columnTypes.add(columnTypeMetadata.getPrimitiveType());
      tableMetadata.columnTypeInfo.put(columnTypeMetadata.key(), columnTypeMetadata);
    }
  }

  private void extractFiles(JsonObject obj) {
    tableMetadata.files = Lists.newArrayList();

    final JsonArray files = obj.get("files").asArray();
    for (int i = 0; i < files.size(); i++) {
      if (i % 1000 == 0) {
        System.out.printf("processing file %d/%d%n", i, files.size());
      }
      tableMetadata.files.add(parseFile(files.get(i).asObject()));
    }
  }

  private ColumnTypeMetadata parseColumnTypeInfo(JsonObject object) {
    final String[] name = Utils.parseStringArray(object.get("name").asArray());

    final JsonValue primitiveTypeValue = object.get("primitiveType");
    PrimitiveTypeName primitiveType = null;
    if (!primitiveTypeValue.isNull()) {
      primitiveType = PrimitiveTypeName.valueOf(primitiveTypeValue.asString());
    }

    final JsonValue originalValue = object.get("originalType");
    OriginalType originalType = null;
    if (!originalValue.isNull()) {
      originalType = OriginalType.valueOf(originalValue.asString());
    }

    return new ColumnTypeMetadata(name, primitiveType, originalType);
  }

  private Holders.ParquetFileMetadata parseFile(final JsonObject obj) {

    final JsonArray rowGroupsArray = obj.get("rowGroups").asArray();
    final List<Holders.RowGroupMetadata> rowGroups = Lists.newArrayList();
    for (int i = 0; i < rowGroupsArray.size(); i++) {
      rowGroups.add(parseRowGroup(rowGroupsArray.get(i).asObject()));
    }

    return new Holders.ParquetFileMetadata(obj.getString("path", ""), obj.getLong("length", -1), rowGroups);
  }

  private Holders.RowGroupMetadata parseRowGroup(final JsonObject obj) {
    final Map<String, Float> affinites = Maps.newHashMap();
    final JsonObject hostAffinity = obj.get("hostAffinity").asObject();
    for (final String name : hostAffinity.names()) {
      affinites.put(name, hostAffinity.getFloat(name, -1));
    }

    final List<Holders.ColumnMetadata> columns = Lists.newArrayList();
    final JsonArray columnsArray = obj.get("columns").asArray();
    for (final JsonValue column : columnsArray) {
      final Holders.ColumnMetadata columnMetadata = parseRowGroupColumn(column.asObject());
      if (columnMetadata != null) {
        columns.add(columnMetadata);
      }
    }

    return new Holders.RowGroupMetadata(obj.getLong("start", -1), obj.getLong("length", -1),
      obj.getLong("rowCount", -1), affinites, columns);
  }

  private PrimitiveTypeName getColumnType(final String[] columnName) {
    final String name = Utils.COLUMN_NAME_JOINER.join(columnName);
    final int nameId = columnNames.indexOf(name);
    if (nameId == -1) {
      throw new RuntimeException("column '" + name + "' not found in columnNames");
    }
    return columnTypes.get(nameId);
  }

  private Holders.ColumnMetadata parseRowGroupColumn(final JsonObject obj) {
    Long nulls = obj.get("nulls").asLong();
    final JsonValue mxJsonValue = obj.get("mxValue");

    if (nulls == 0 && mxJsonValue == null) {
      return null;
    }

    final String[] columnName = Utils.parseStringArray(obj.get("name").asArray());
    final PrimitiveTypeName primitiveType = getColumnType(columnName);

    if (nulls > 0) {
      nulls = null;
    }

    Object mxValue = null;
    if (mxJsonValue != null) {
      switch (primitiveType) {
        case INT64:
          mxValue = mxJsonValue.asLong();
          break;
        case INT32:
          mxValue = mxJsonValue.asInt();
          break;
        case BOOLEAN:
          mxValue = mxJsonValue.asBoolean();
          break;
        case BINARY:
        case FIXED_LEN_BYTE_ARRAY:
        case INT96:
          mxValue = ByteString.copyFrom(mxJsonValue.asString().getBytes());
          break;
        case FLOAT:
          mxValue = mxJsonValue.asFloat();
          break;
        case DOUBLE:
          mxValue = mxJsonValue.asDouble();
          break;
      }
    }

    return new Holders.ColumnMetadata(columnName, primitiveType, mxValue, nulls);
  }
}
