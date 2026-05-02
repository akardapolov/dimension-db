package ru.dimension.db.model;

public enum GranularityFunction {
  AUTO,
  MINUTE,
  HOUR,
  DAY,
  WEEK,
  MONTH;

  public long getMillis() {
    return switch (this) {
      case MINUTE -> 60_000L;
      case HOUR -> 3_600_000L;
      case DAY -> 86_400_000L;
      case WEEK -> 604_800_000L;
      case MONTH -> 2_592_000_000L;
      case AUTO -> 0L;
    };
  }

  public static GranularityFunction resolve(GranularityFunction requested,
                                            long begin,
                                            long end) {
    if (requested != AUTO) {
      return requested;
    }

    long rangeMs = end - begin;

    if (rangeMs <= 3_600_000L) {
      return MINUTE;
    } else if (rangeMs <= 86_400_000L) {
      return HOUR;
    } else if (rangeMs <= 7 * 86_400_000L) {
      return DAY;
    } else if (rangeMs <= 30 * 86_400_000L) {
      return DAY;
    } else {
      return WEEK;
    }
  }
}
