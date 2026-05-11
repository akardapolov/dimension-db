package ru.dimension.db.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class PercentileUtil {

  public static final int MIN_BUCKETS_FOR_PERCENTILE = 5;

  private PercentileUtil() {}

  public static double compute(List<Double> values, double p) {
    if (values == null || values.isEmpty()) {
      return 0.0;
    }

    List<Double> sorted = new ArrayList<>(values);
    Collections.sort(sorted);

    int index = (int) Math.ceil(p * sorted.size()) - 1;
    return sorted.get(Math.max(0, index));
  }

  public static List<long[]> buildBuckets(long begin, long end, long bucketSize) {
    List<long[]> buckets = new ArrayList<>();
    if (bucketSize <= 0) {
      buckets.add(new long[]{begin, end});
      return buckets;
    }

    long cursor = begin;
    while (cursor < end) {
      long bucketEnd = Math.min(cursor + bucketSize, end);
      buckets.add(new long[]{cursor, bucketEnd});
      cursor = bucketEnd;
    }

    return buckets;
  }
}
