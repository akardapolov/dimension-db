package ru.dimension.db.service;

import ru.dimension.db.model.profile.cstype.SType;

public interface StatisticsService {

  boolean isStatByTableExist(byte tableId);
  SType getLastSType(byte tableId, int colId, boolean isTimestamp);
}
