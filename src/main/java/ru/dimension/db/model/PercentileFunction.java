package ru.dimension.db.model;

public enum PercentileFunction {
  NONE,
  P50,
  P90,
  P95,
  P99;

  public double getValue() {
    return switch (this) {
      case P50 -> 0.50;
      case P90 -> 0.90;
      case P95 -> 0.95;
      case P99 -> 0.99;
      case NONE -> 0.0;
    };
  }
}
