package ru.dimension.db.storage;

public interface DimensionDAO {

  int getOrLoad(double value);

  int getOrLoad(String value);

  String getStringById(int key);

  double getDoubleById(int key);
}
