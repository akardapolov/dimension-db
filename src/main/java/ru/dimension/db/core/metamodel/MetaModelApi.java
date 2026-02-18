package ru.dimension.db.core.metamodel;

import java.util.List;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.table.AType;
import ru.dimension.db.model.profile.table.BType;
import ru.dimension.db.model.profile.table.IType;
import ru.dimension.db.model.profile.table.TType;

public interface MetaModelApi {

  byte getTableId(String tableName);

  String getTableName(byte tableId);

  List<CProfile> getCProfiles(String tableName);

  CProfile getTimestampCProfile(String tableName);

  BType getBackendType(String tableName);

  TType getTableType(String tableName);

  IType getIndexType(String tableName);
  AType getAnalyzeType(String tableName);

  Boolean getTableCompression(String tableName);
}