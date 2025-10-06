// MapArrayUtil.java remains unchanged
package ru.dimension.db.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.experimental.UtilityClass;

@UtilityClass
public class MapArrayUtil {

  public static String arrayToString(Object obj) {
    return switch (obj) {
      case long[] longArray -> Arrays.toString(longArray);
      case int[] intArray -> Arrays.toString(intArray);
      case short[] shortArray -> Arrays.toString(shortArray);
      case byte[] byteArray -> Arrays.toString(byteArray);
      case double[] doubleArray -> Arrays.toString(doubleArray);
      case String[] stringArray -> Arrays.toString(stringArray);
      case null, default -> throw new IllegalArgumentException("Object is not an array of a supported type.");
    };
  }

  @FunctionalInterface
  public interface KeyParser<K> {

    K parse(String input);
  }

  @FunctionalInterface
  public interface ValueParser<V> {

    V parse(String input);
  }

  public static <K, V> Map<K, V> parseStringToTypedMap(String input,
                                                       KeyParser<K> keyParser,
                                                       ValueParser<V> valueParser,
                                                       String KVDelimiter) {
    Map<K, V> map = new HashMap<>();
    Pattern p = Pattern.compile("([\\w]+)" + KVDelimiter + "([0-9]+\\.?[0-9]*)");
    Matcher m = p.matcher(input);

    while (m.find()) {
      K key = keyParser.parse(m.group(1));
      V value = valueParser.parse(m.group(2));
      map.put(key, value);
    }

    return map;
  }

  public static String[] parseStringToTypedArray(String input,
                                                 String d) {
    String dsv = input.replace("[", "").replace("]", "");

    return dsv.split(d);
  }

  public static String mapToJson(Map<?, ?> map) {
    StringJoiner sj = new StringJoiner(",", "{", "}");
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      String key = String.valueOf(entry.getKey());
      String value = toJsonValue(entry.getValue());
      sj.add(toJsonString(key) + ":" + value);
    }
    return sj.toString();
  }

  public static String mapToJsonRaw(Map<?, ?> map) {
    StringJoiner sj_map = new StringJoiner(", ", "{", "}");
    map.forEach((key, value) -> sj_map.add("'" + key + "':" + value));
    return sj_map.toString();
  }

  private static String toJsonValue(Object value) {
    if (value instanceof Map<?, ?>) {
      return mapToJson((Map<?, ?>) value);
    } else if (value instanceof String) {
      return toJsonString((String) value);
    } else if (value instanceof Number) {
      return value.toString();
    } else if (value instanceof Boolean) {
      return value.toString();
    } else if (value == null) {
      return "null";
    } else {
      return toJsonString(value.toString());
    }
  }

  private static String toJsonString(String value) {
    return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }
}