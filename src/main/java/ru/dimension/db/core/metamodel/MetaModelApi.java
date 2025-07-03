package ru.dimension.db.core.metamodel;

import java.util.List;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.table.BType;
import ru.dimension.db.model.profile.table.IType;
import ru.dimension.db.model.profile.table.TType;

public interface MetaModelApi {

  byte getTableId(String tableName);
  String getTableName(Byte tableId);

  TType getTableType(String tableName);

  IType getIndexType(String tableName);

  BType getBackendType(String tableName);

  Boolean getTableCompression(String tableName);

  List<CProfile> getCProfiles(String tableName);

  List<CProfile> getCProfiles(Byte tableId);

  CProfile getTimestampCProfile(String tableName);
}
