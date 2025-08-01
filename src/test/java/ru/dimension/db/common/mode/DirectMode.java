package ru.dimension.db.common.mode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import ru.dimension.db.exception.TableNameEmptyException;
import ru.dimension.db.metadata.DataType;
import ru.dimension.db.model.DBaseTestConfig;
import ru.dimension.db.model.profile.SProfile;
import ru.dimension.db.model.profile.TProfile;
import ru.dimension.db.model.profile.cstype.CSType;
import ru.dimension.db.model.profile.cstype.CType;
import ru.dimension.db.model.profile.cstype.SType;
import ru.dimension.db.model.profile.table.AType;
import ru.dimension.db.model.profile.table.BType;
import ru.dimension.db.model.profile.table.IType;
import ru.dimension.db.model.profile.table.TType;

public interface DirectMode {

  default SProfile getSProfileDirect(
      String tableName,
      TType tableType,
      IType indexType,
      AType analyzeType,
      BType backendType,
      boolean compression
  ) {
    SProfile sProfile = SProfile.builder()
        .tableName(tableName)
        .tableType(tableType)
        .indexType(indexType)
        .analyzeType(analyzeType)
        .backendType(backendType)
        .compression(compression)
        .csTypeMap(new LinkedHashMap<>()).build();

    Map<String, CSType> csTypeMap = new LinkedHashMap<>();
    csTypeMap.put("ID", CSType.builder().isTimeStamp(true).sType(SType.RAW).cType(CType.LONG).dType(DataType.LONG).build());
    csTypeMap.put("LONG_FIELD", CSType.builder().isTimeStamp(false).sType(SType.RAW).cType(CType.LONG).dType(DataType.LONG).build());
    csTypeMap.put("DOUBLE_FIELD", CSType.builder().isTimeStamp(false).sType(SType.RAW).cType(CType.DOUBLE).dType(DataType.DOUBLE).build());
    csTypeMap.put("STRING_FIELD", CSType.builder().isTimeStamp(false).sType(SType.RAW).cType(CType.STRING).dType(DataType.STRING).build());

    sProfile.setCsTypeMap(csTypeMap);

    return sProfile;
  }

  default void loadMetadataDirect(
      DBaseTestConfig config,
      String tableName,
      TType tableType,
      IType indexType,
      AType analyzeType,
      BType backendType,
      boolean compression
  ) throws TableNameEmptyException {
    SProfile sProfile = getSProfileDirect(tableName, tableType, indexType, analyzeType, backendType, compression);
    config.getDStore().loadDirectTableMetadata(sProfile);
  }

  default TProfile setupDirectProfile(String tableName, DBaseTestConfig configDirect) throws TableNameEmptyException {
    SProfile sProfile = getSProfileDirect(tableName,
                                          TType.REGULAR,
                                          IType.GLOBAL,
                                          AType.ON_LOAD,
                                          BType.BERKLEYDB,
                                          true);
    return configDirect.getDStore().loadDirectTableMetadata(sProfile);
  }

  default List<List<Object>> prepareData() {
    return Arrays.asList(
        addValue(1707387748310L),
        addValue(17073877482L),
        addValue(17073877482D),
        addValue("a31bce67-d1ab-485b-9ffa-850385e298ac")
    );
  }

  default <T> ArrayList<T> addValue(T value) {
    ArrayList<T> list = new ArrayList<>(1);
    list.add(value);
    return list;
  }
}
