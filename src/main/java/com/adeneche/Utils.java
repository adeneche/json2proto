package com.adeneche;

import com.eclipsesource.json.JsonArray;
import com.google.common.base.Joiner;

public class Utils {
  public static final Joiner COLUMN_NAME_JOINER = Joiner.on(".");

  static String[] parseStringArray(final JsonArray array) {
    final String[] strings = new String[array.size()];
    for (int i = 0; i < array.size(); i++) {
      strings[i] = array.get(i).asString();
    }
    return strings;
  }

}
