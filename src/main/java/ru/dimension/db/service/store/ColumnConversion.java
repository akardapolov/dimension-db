package ru.dimension.db.service.store;

import java.util.List;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.cstype.CType;
import ru.dimension.db.model.profile.cstype.SType;

public class ColumnConversion {
  final int colId;
  final CProfile cProfile;
  final SType newType;
  final List<Integer> intValues;
  final CType cType;

  ColumnConversion(int colId, CProfile cProfile, SType newType, List<Integer> intValues, CType cType) {
    this.colId = colId;
    this.cProfile = cProfile;
    this.newType = newType;
    this.intValues = intValues;
    this.cType = cType;
  }
}
