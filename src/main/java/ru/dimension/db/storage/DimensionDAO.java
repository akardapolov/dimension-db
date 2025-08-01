package ru.dimension.db.storage;

public interface DimensionDAO {
  int getOrLoad(double value);
  int getOrLoad(String value);
  int getOrLoad(long value);

  String getStringById(int key);
  double getDoubleById(int key);
  long getLongById(int key);
}