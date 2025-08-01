package ru.dimension.db;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import ru.dimension.db.common.AbstractDirectTest;
import ru.dimension.db.exception.BeginEndWrongOrderException;
import ru.dimension.db.exception.GanttColumnNotSupportedException;
import ru.dimension.db.exception.SqlColMetadataException;
import ru.dimension.db.metadata.DataType;
import ru.dimension.db.model.GroupFunction;
import ru.dimension.db.model.output.GanttColumnCount;
import ru.dimension.db.model.output.StackedColumn;
import ru.dimension.db.model.profile.SProfile;
import ru.dimension.db.model.profile.cstype.CSType;
import ru.dimension.db.model.profile.cstype.CType;
import ru.dimension.db.model.profile.cstype.SType;
import ru.dimension.db.model.profile.table.AType;
import ru.dimension.db.model.profile.table.BType;
import ru.dimension.db.model.profile.table.IType;
import ru.dimension.db.model.profile.table.TType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class DBase13LongDoubleDirectTest extends AbstractDirectTest {

  @BeforeAll
  public void init() {
    SProfile sProfile = new SProfile();
    sProfile.setTableName(tableName);
    sProfile.setTableType(TType.TIME_SERIES);
    sProfile.setIndexType(IType.GLOBAL);
    sProfile.setAnalyzeType(AType.ON_LOAD);
    sProfile.setBackendType(BType.BERKLEYDB);
    sProfile.setCompression(true);

    Map<String, CSType> csTypeMap = new LinkedHashMap<>();
    csTypeMap.put("ID", CSType.builder().isTimeStamp(true).sType(SType.RAW).cType(CType.LONG).dType(DataType.LONG).build());
    csTypeMap.put("LONG_FIELD", CSType.builder().isTimeStamp(false).sType(SType.RAW).cType(CType.LONG).dType(DataType.LONG).build());
    csTypeMap.put("DOUBLE_FIELD", CSType.builder().isTimeStamp(false).sType(SType.RAW).cType(CType.DOUBLE).dType(DataType.DOUBLE).build());
    csTypeMap.put("STRING_FIELD", CSType.builder().isTimeStamp(false).sType(SType.RAW).cType(CType.STRING).dType(DataType.STRING).build());

    sProfile.setCsTypeMap(csTypeMap);

    putDataSimpleDirect(sProfile);
  }

  @Test
  public void computeStackedTest() throws BeginEndWrongOrderException, SqlColMetadataException, IOException {
    List<StackedColumn> longField = getDataStackedColumn("LONG_FIELD", GroupFunction.COUNT, Long.MIN_VALUE, Long.MAX_VALUE);
    List<StackedColumn> doubleField = getDataStackedColumn("DOUBLE_FIELD", GroupFunction.COUNT, Long.MIN_VALUE, Long.MAX_VALUE);
    List<StackedColumn> stringField = getDataStackedColumn("STRING_FIELD", GroupFunction.COUNT, Long.MIN_VALUE, Long.MAX_VALUE);

    List<StackedColumn> expected = getStackedDataExpected("long_double_string_stacked.json");
    List<StackedColumn> actual = Stream.of(longField.stream(),
                                           doubleField.stream(),
                                           stringField.stream())
        .flatMap(Function.identity())
        .collect(Collectors.toList());

    assertEquals(expected, actual);
  }

  @Test
  public void computeGanttTest()
      throws BeginEndWrongOrderException, SqlColMetadataException, GanttColumnNotSupportedException, IOException {
    List<GanttColumnCount> longDouble = getDataGanttColumn("LONG_FIELD", "DOUBLE_FIELD", Long.MIN_VALUE, Long.MAX_VALUE);
    List<GanttColumnCount> doubleLong = getDataGanttColumn("DOUBLE_FIELD", "LONG_FIELD", Long.MIN_VALUE, Long.MAX_VALUE);
    List<GanttColumnCount> doubleString = getDataGanttColumn("DOUBLE_FIELD", "STRING_FIELD", Long.MIN_VALUE, Long.MAX_VALUE);
    List<GanttColumnCount> longString = getDataGanttColumn("LONG_FIELD", "STRING_FIELD", Long.MIN_VALUE, Long.MAX_VALUE);
    List<GanttColumnCount> stringLong = getDataGanttColumn("STRING_FIELD", "LONG_FIELD", Long.MIN_VALUE, Long.MAX_VALUE);
    List<GanttColumnCount> stringDouble = getDataGanttColumn("STRING_FIELD", "DOUBLE_FIELD", Long.MIN_VALUE, Long.MAX_VALUE);

    List<GanttColumnCount> expected = getGanttDataExpected("long_double_string_gantt.json");
    List<GanttColumnCount> actual = Stream.of(longDouble.stream(),
                                              doubleLong.stream(),
                                              doubleString.stream(),
                                              longString.stream(),
                                              stringLong.stream(),
                                              stringDouble.stream())
        .flatMap(Function.identity())
        .collect(Collectors.toList());

    assertEquals(expected, actual);
  }

  @Test
  public void computeRawTest() {
    List<List<Object>> expected = List.of(Arrays.asList(startTime, longValue, doubleValue, stringValue));

    List<List<Object>> actual = getRawDataAll(Long.MIN_VALUE, Long.MAX_VALUE);

    assertEquals(expected.toString(), actual.toString());
  }
}
