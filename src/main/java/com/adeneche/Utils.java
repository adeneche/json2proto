package com.adeneche;

import com.adeneche.metadata.Metadata;
import com.eclipsesource.json.JsonArray;
import com.google.common.base.Joiner;

import java.util.Collections;

public class Utils {
  public static final Joiner COLUMN_NAME_JOINER = Joiner.on(".");

  static String[] parseStringArray(final JsonArray array) {
    final String[] strings = new String[array.size()];
    for (int i = 0; i < array.size(); i++) {
      strings[i] = array.get(i).asString();
    }
    return strings;
  }

  public static void findDifference(Metadata.MetadataHeader header1, Metadata.MetadataHeader header2) {
    if (header1.equals(header2)) {
      return;
    }

    if (!header1.getMetadataVersion().equals(header2.getMetadataVersion())) {
      System.out.println("header.metadata_version");
    }

    if (!header1.getDirectoriesList().equals(header2.getDirectoriesList())) {
      System.out.println("header.directories");
    }
  }

  public static void findDifference(Metadata.MetadataFiles files1, Metadata.MetadataFiles files2) {
    if (files1.equals(files2)) {
      return;
    }

    if (files1.getColumnsCount() != files2.getColumnsCount()) {
      System.out.printf("files.columnsCount: %d vs %d%n", files1.getColumnsCount(), files2.getColumnsCount());
    } else if (files1.getColumnsList().equals(files2.getColumnsList())){

    }

    if (files1.getFilesCount() != files2.getFilesCount()) {
      System.out.printf("files.filesCount: %d vs %d%n", files1.getFilesCount(), files2.getFilesCount());
    } else {

    }
  }
}
