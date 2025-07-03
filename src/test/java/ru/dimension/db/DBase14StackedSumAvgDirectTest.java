package ru.dimension.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import ru.dimension.db.common.AbstractDirectTest;
import ru.dimension.db.exception.BeginEndWrongOrderException;
import ru.dimension.db.exception.SqlColMetadataException;
import ru.dimension.db.metadata.DataType;
import ru.dimension.db.model.GroupFunction;
import ru.dimension.db.model.output.StackedColumn;
import ru.dimension.db.model.profile.SProfile;
import ru.dimension.db.model.profile.cstype.CSType;
import ru.dimension.db.model.profile.cstype.CType;
import ru.dimension.db.model.profile.cstype.SType;
import ru.dimension.db.model.profile.table.BType;
import ru.dimension.db.model.profile.table.IType;
import ru.dimension.db.model.profile.table.TType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class DBase14StackedSumAvgDirectTest extends AbstractDirectTest {

  @BeforeAll
  public void init() {
    SProfile sProfile = new SProfile();
    sProfile.setTableName(tableName);
    sProfile.setTableType(TType.TIME_SERIES);
    sProfile.setIndexType(IType.GLOBAL);
    sProfile.setBackendType(BType.BERKLEYDB);
    sProfile.setCompression(true);

    Map<String, CSType> csTypeMap = new LinkedHashMap<>();
    csTypeMap.put("ID", CSType.builder().isTimeStamp(true).sType(SType.RAW).cType(CType.LONG).dType(DataType.LONG).build());
    csTypeMap.put("LONG_FIELD", CSType.builder().isTimeStamp(false).sType(SType.RAW).cType(CType.LONG).dType(DataType.LONG).build());
    csTypeMap.put("DOUBLE_FIELD", CSType.builder().isTimeStamp(false).sType(SType.RAW).cType(CType.DOUBLE).dType(DataType.DOUBLE).build());
    csTypeMap.put("STRING_FIELD", CSType.builder().isTimeStamp(false).sType(SType.RAW).cType(CType.STRING).dType(DataType.STRING).build());

    sProfile.setCsTypeMap(csTypeMap);

    putDataGroupFunctionsDirect(sProfile);
  }

  @Test
  public void computeStackedTest() throws BeginEndWrongOrderException, SqlColMetadataException {
    List<StackedColumn> longSumField = getDataStackedColumn("LONG_FIELD", GroupFunction.SUM, Long.MIN_VALUE, Long.MAX_VALUE);
    List<StackedColumn> longAvgField = getDataStackedColumn("LONG_FIELD", GroupFunction.AVG, Long.MIN_VALUE, Long.MAX_VALUE);
    List<StackedColumn> doubleSumField = getDataStackedColumn("DOUBLE_FIELD", GroupFunction.SUM, Long.MIN_VALUE, Long.MAX_VALUE);
    List<StackedColumn> doubleAvgField = getDataStackedColumn("DOUBLE_FIELD", GroupFunction.AVG, Long.MIN_VALUE, Long.MAX_VALUE);

    Double longSum = longSumField.stream().findAny().orElseThrow().getKeySum().get("LONG_FIELD");
    Double longAvg = longAvgField.stream().findAny().orElseThrow().getKeyAvg().get("LONG_FIELD");

    Double doubleSum = doubleSumField.stream().findAny().orElseThrow().getKeySum().get("DOUBLE_FIELD");
    Double doubleAvg = doubleAvgField.stream().findAny().orElseThrow().getKeyAvg().get("DOUBLE_FIELD");

    assertEquals(longValue * 2, longSum);
    assertEquals(longValue, longAvg);

    assertEquals(doubleValue * 2, doubleSum);
    assertEquals(doubleValue, doubleAvg);
  }

  @Test
  public void computeStackedStringSumThrowsTest() {
    assertThrows(RuntimeException.class,
                 ()-> getDataStackedColumn("STRING_FIELD", GroupFunction.SUM, Long.MIN_VALUE, Long.MAX_VALUE));
  }

  @Test
  public void computeStackedStringAvgThrowsTest() {
    assertThrows(RuntimeException.class,
                 ()-> getDataStackedColumn("STRING_FIELD", GroupFunction.AVG, Long.MIN_VALUE, Long.MAX_VALUE));
  }
}
