package ru.dimension.db.storage.format;

public interface DualColumnProcessor {
  void process(String firstValue, String secondValue);
  void process(int firstValue, int secondValue);
  void process(String firstValue, int secondValue);
  void process(int firstValue, String secondValue);
}