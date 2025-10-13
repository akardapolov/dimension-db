package ru.dimension.db.storage.format;

import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.cstype.SType;

public interface StorageReader {
  String getName();
  boolean supports(SType sType);

  String[] getStringValues(StorageContext context, CProfile cProfile);
  double[] getDoubleValues(StorageContext context, CProfile cProfile);

  default int[] getIntValues(StorageContext context, CProfile cProfile) {
    throw new UnsupportedOperationException("Int values not supported for format: " + getName());
  }
}