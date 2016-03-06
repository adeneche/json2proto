package com.adeneche;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.adeneche.metadata.Metadata.MetadataColumns.ColumnTypeInfo.PrimitiveTypeName;
import com.adeneche.metadata.Metadata.MetadataColumns.ColumnTypeInfo.OriginalType;

/**
 * Structures similar to Drill's Metadata cache
 */
public class Holders {

  /**
   * Struct which contains the metadata for an entire parquet directory structure
   */
  public static class ParquetTableMetadata {
    /*
     ColumnTypeInfo is schema information from all the files and row groups, merged into
     one. To get this info, we pass the ParquetTableMetadata object all the way dow to the
     RowGroup and the column type is built there as it is read from the footer.
     */
    public Map<ColumnTypeMetadata.Key, ColumnTypeMetadata> columnTypeInfo;
    List<ParquetFileMetadata> files;
    List<String> directories;

    public ColumnTypeMetadata getColumnTypeInfo(String[] name) {
      return columnTypeInfo.get(new ColumnTypeMetadata.Key(name));
    }

    public List<String> getDirectories() {
      return directories;
    }

    public List<? extends ParquetFileMetadata> getFiles() {
      return files;
    }

    public boolean hasColumnMetadata() {
      return true;
    }

    public PrimitiveTypeName getPrimitiveType(String[] columnName) {
      return getColumnTypeInfo(columnName).primitiveType;
    }

    public OriginalType getOriginalType(String[] columnName) {
      return getColumnTypeInfo(columnName).originalType;
    }
  }

  public static class ColumnTypeMetadata {
    public String[] name;
    public PrimitiveTypeName primitiveType;
    public OriginalType originalType;

    // Key to find by name only
    private Key key;

    public ColumnTypeMetadata() {
      super();
    }

    public ColumnTypeMetadata(String[] name, PrimitiveTypeName primitiveType, OriginalType originalType) {
      this.name = name;
      this.primitiveType = primitiveType;
      this.originalType = originalType;
      this.key = new Key(name);
    }

    public String[] getName() {
      return name;
    }

    public PrimitiveTypeName getPrimitiveType() {
      return primitiveType;
    }

    public Key key() {
      return this.key;
    }

    public static class Key {
      private String[] name;
      private int hashCode = 0;

      public Key(String[] name) {
        this.name = name;
      }

      @Override public int hashCode() {
        if (hashCode == 0) {
          hashCode = Arrays.hashCode(name);
        }
        return hashCode;
      }

      @Override public boolean equals(Object obj) {
        if (obj == null) {
          return false;
        }
        if (getClass() != obj.getClass()) {
          return false;
        }
        final Key other = (Key) obj;
        return Arrays.equals(this.name, other.name);
      }

      @Override public String toString() {
        String s = null;
        for (String namePart : name) {
          if (s != null) {
            s += ".";
            s += namePart;
          } else {
            s = namePart;
          }
        }
        return s;
      }

    }
  }


  /**
   * Struct which contains the metadata for a single parquet file
   */
  public static class ParquetFileMetadata {
    public String path;
    public Long length;
    public List<RowGroupMetadata> rowGroups;

    public ParquetFileMetadata() {
      super();
    }

    public ParquetFileMetadata(String path, Long length, List<RowGroupMetadata> rowGroups) {
      this.path = path;
      this.length = length;
      this.rowGroups = rowGroups;
    }

    public String toString() {
      return String.format("path: %s rowGroups: %s", path, rowGroups);
    }

    public String getPath() {
      return path;
    }

    public Long getLength() {
      return length;
    }

    public List<? extends RowGroupMetadata> getRowGroups() {
      return rowGroups;
    }
  }


  /**
   * A struct that contains the metadata for a parquet row group
   */
  public static class RowGroupMetadata {
    public Long start;
    public Long length;
    public Long rowCount;
    public Map<String, Float> hostAffinity;
    public List<ColumnMetadata> columns;

    public RowGroupMetadata() {
      super();
    }

    public RowGroupMetadata(Long start, Long length, Long rowCount, Map<String, Float> hostAffinity,
                            List<ColumnMetadata> columns) {
      this.start = start;
      this.length = length;
      this.rowCount = rowCount;
      this.hostAffinity = hostAffinity;
      this.columns = columns;
    }

    public Long getStart() {
      return start;
    }

    public Long getLength() {
      return length;
    }

    public Long getRowCount() {
      return rowCount;
    }

    public Map<String, Float> getHostAffinity() {
      return hostAffinity;
    }

    public List<? extends ColumnMetadata> getColumns() {
      return columns;
    }
  }

  /**
   * A struct that contains the metadata for a column in a parquet file
   */
  public static class ColumnMetadata {
    // Use a string array for name instead of Schema Path to make serialization easier
    public String[] name;
    public Long nulls;

    public Object mxValue;

    private PrimitiveTypeName primitiveType;

    public ColumnMetadata(String[] name, PrimitiveTypeName primitiveType, Object mxValue, Long nulls) {
      this.name = name;
      this.mxValue = mxValue;
      this.nulls = nulls;
      this.primitiveType = primitiveType;
    }

    public void setMax(Object mxValue) {
      this.mxValue = mxValue;
    }

    public String[] getName() {
      return name;
    }

    public Long getNulls() {
      return nulls;
    }

    public boolean hasSingleValue() {
      return (mxValue != null);
    }

    public Object getMaxValue() {
      return mxValue;
    }

    public PrimitiveTypeName getPrimitiveType() {
      return null;
    }

    public OriginalType getOriginalType() {
      return null;
    }
  }
}
