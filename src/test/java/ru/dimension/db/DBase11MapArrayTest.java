package ru.dimension.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static ru.dimension.db.service.CommonServiceApi.transpose;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.dimension.db.common.AbstractDirectTest;
import ru.dimension.db.exception.BeginEndWrongOrderException;
import ru.dimension.db.exception.GanttColumnNotSupportedException;
import ru.dimension.db.exception.SqlColMetadataException;
import ru.dimension.db.metadata.DataType;
import ru.dimension.db.model.CompareFunction;
import ru.dimension.db.model.GroupFunction;
import ru.dimension.db.model.OrderBy;
import ru.dimension.db.model.filter.CompositeFilter;
import ru.dimension.db.model.filter.FilterCondition;
import ru.dimension.db.model.filter.LogicalOperator;
import ru.dimension.db.model.output.GanttColumnCount;
import ru.dimension.db.model.output.GanttColumnSum;
import ru.dimension.db.model.output.StackedColumn;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.SProfile;
import ru.dimension.db.model.profile.cstype.CSType;
import ru.dimension.db.model.profile.cstype.CType;
import ru.dimension.db.model.profile.cstype.SType;
import ru.dimension.db.model.profile.table.AType;
import ru.dimension.db.model.profile.table.BType;
import ru.dimension.db.model.profile.table.IType;
import ru.dimension.db.model.profile.table.TType;

public class DBase11MapArrayTest extends AbstractDirectTest {

  @BeforeAll
  public void init() {
    SProfile sProfile = new SProfile();
    sProfile.setTableName(tableName);
    sProfile.setTableType(TType.TIME_SERIES);
    sProfile.setIndexType(IType.GLOBAL);
    sProfile.setAnalyzeType(AType.ON_LOAD);
    sProfile.setBackendType(BType.BERKLEYDB);
    sProfile.setCompression(true);

    Map<String, CSType> csTypeMap = new HashMap<>();
    csTypeMap.put("ID", CSType.builder().isTimeStamp(true).sType(SType.RAW).cType(CType.LONG).dType(DataType.LONG).build());
    csTypeMap.put("MESSAGE", CSType.builder().isTimeStamp(false).sType(SType.RAW).cType(CType.STRING).dType(DataType.VARCHAR).build());
    csTypeMap.put("MAP", CSType.builder().isTimeStamp(false).sType(SType.RAW).cType(CType.STRING).dType(DataType.MAP).build());
    csTypeMap.put("ARRAY", CSType.builder().isTimeStamp(false).sType(SType.RAW).cType(CType.STRING).dType(DataType.ARRAY).build());
    csTypeMap.put("LONG_COL", CSType.builder().isTimeStamp(false).sType(SType.RAW).cType(CType.LONG).dType(DataType.LONG).build());

    sProfile.setCsTypeMap(csTypeMap);

    putDataDirect(sProfile);
  }

  @Test
  public void computeStackedTest() throws BeginEndWrongOrderException, SqlColMetadataException {
    List<StackedColumn> listMessage = getDataStackedColumn("MESSAGE", GroupFunction.COUNT, Integer.MIN_VALUE, Integer.MAX_VALUE);
    List<StackedColumn> listMap = getDataStackedColumn("MAP", GroupFunction.COUNT, Integer.MIN_VALUE, Integer.MAX_VALUE);
    List<StackedColumn> listArray = getDataStackedColumn("ARRAY", GroupFunction.COUNT, Integer.MIN_VALUE, Integer.MAX_VALUE);

    assertEquals(findListStackedKey(listMessage, testMessage1), testMessage1);
    assertEquals(findListStackedValue(listMessage, testMessage1), 2);

    assertEquals(findListStackedKey(listMessage, testMessage2), testMessage2);
    assertEquals(findListStackedValue(listMessage, testMessage2), 2);

    compareKeySetForMapDataType(testMap1, listMap);
    compareKeySetForMapDataType(testMap2, listMap);

    Map<String, Integer> expectedTestMap3Aggregated = new HashMap<>();
    expectedTestMap3Aggregated.put("val7", 14);
    expectedTestMap3Aggregated.put("val8", 16);
    expectedTestMap3Aggregated.put("val9", 18);

    assertEquals(expectedTestMap3Aggregated, listMap.stream()
        .filter(f -> f.getKeyCount().containsKey("val7"))
        .findAny().orElseThrow().getKeyCount());

    Map<String, Integer> totalArrayCounts = listArray.stream()
        .flatMap(sc -> sc.getKeyCount().entrySet().stream())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Integer::sum));

    assertEquals(6, totalArrayCounts.get("array value 1"));
    assertEquals(6, totalArrayCounts.get("array value 2"));
  }

  @Test
  public void computeStackedTestWithMapFilter() throws SqlColMetadataException, BeginEndWrongOrderException {
    CProfile mapProfile = cProfiles.stream().filter(p -> p.getColName().equals("MAP")).findFirst().orElseThrow();
    FilterCondition condition = new FilterCondition(mapProfile, new String[]{"val4"}, CompareFunction.EQUAL);
    CompositeFilter filter = new CompositeFilter(List.of(condition), LogicalOperator.AND);

    List<StackedColumn> listMessage = dStore.getStacked(tableName,
                                                        cProfiles.stream().filter(p -> p.getColName().equals("MESSAGE")).findFirst().orElseThrow(),
                                                        GroupFunction.COUNT, filter, Integer.MIN_VALUE, Integer.MAX_VALUE);

    assertEquals(1, listMessage.size());
    Map<String, Integer> counts = listMessage.get(0).getKeyCount();
    assertEquals(2, counts.get(testMessage2));
    assertFalse(counts.containsKey(testMessage1));
    assertFalse(counts.containsKey(testMessage3));
  }

  @Test
  public void computeStackedTestWithArrayFilter() throws SqlColMetadataException, BeginEndWrongOrderException {
    CProfile arrayProfile = cProfiles.stream().filter(p -> p.getColName().equals("ARRAY")).findFirst().orElseThrow();
    FilterCondition condition = new FilterCondition(arrayProfile, new String[]{"array value 1"}, CompareFunction.EQUAL);
    CompositeFilter filter = new CompositeFilter(List.of(condition), LogicalOperator.AND);

    List<StackedColumn> listMessage = dStore.getStacked(tableName,
                                                        cProfiles.stream().filter(p -> p.getColName().equals("MESSAGE")).findFirst().orElseThrow(),
                                                        GroupFunction.COUNT, filter, Integer.MIN_VALUE, Integer.MAX_VALUE);

    Map<String, Integer> totalCounts = listMessage.stream()
        .flatMap(sc -> sc.getKeyCount().entrySet().stream())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Integer::sum));

    assertEquals(2, totalCounts.get(testMessage1));
    assertEquals(2, totalCounts.get(testMessage2));
    assertEquals(2, totalCounts.get(testMessage3));
  }

  @Test
  public void computeStackedTestWithFilterOnAggregatedMapColumn()
      throws SqlColMetadataException, BeginEndWrongOrderException {
    CProfile mapProfile = cProfiles.stream().filter(p -> p.getColName().equals("MAP")).findFirst().orElseThrow();
    FilterCondition condition = new FilterCondition(mapProfile, new String[]{"val8"}, CompareFunction.EQUAL);
    CompositeFilter filter = new CompositeFilter(List.of(condition), LogicalOperator.AND);

    List<StackedColumn> listMap = dStore.getStacked(tableName, mapProfile,
                                                    GroupFunction.COUNT, filter, Integer.MIN_VALUE, Integer.MAX_VALUE);

    assertEquals(1, listMap.size());
    Map<String, Integer> counts = listMap.getFirst().getKeyCount();
    assertNotNull(counts);
    assertEquals(1, counts.size());
    assertTrue(counts.containsKey("val8"));
    assertEquals(16, counts.get("val8"));
  }

  @Test
  public void computeGanttTest() throws BeginEndWrongOrderException, SqlColMetadataException, GanttColumnNotSupportedException {
    List<GanttColumnCount> actualMapMessage = getDataGanttColumn("MAP", "MESSAGE", Integer.MIN_VALUE, Integer.MAX_VALUE);
    List<GanttColumnCount> actualArrayMap = getDataGanttColumn("ARRAY", "MAP", Integer.MIN_VALUE, Integer.MAX_VALUE);

    assertGanttDataEqualsIgnoreOrder(getTestDataMapMessage(), actualMapMessage);
    assertGanttDataEqualsIgnoreOrder(getTestArrayMap(), actualArrayMap);

    List<GanttColumnCount> actualMessageMap = getDataGanttColumn("MESSAGE", "MAP", Integer.MIN_VALUE, Integer.MAX_VALUE);
    List<GanttColumnCount> actualMapMap = getDataGanttColumn("MAP", "MAP", Integer.MIN_VALUE, Integer.MAX_VALUE);
    List<GanttColumnCount> actualMapArray = getDataGanttColumn("MAP", "ARRAY", Integer.MIN_VALUE, Integer.MAX_VALUE);
    List<GanttColumnCount> actualArrayArray = getDataGanttColumn("ARRAY", "ARRAY", Integer.MIN_VALUE, Integer.MAX_VALUE);
  }

  @Test
  public void computeGanttWithMapFilterTest() throws BeginEndWrongOrderException, SqlColMetadataException, GanttColumnNotSupportedException {
    CProfile mapProfile = cProfiles.stream().filter(p -> p.getColName().equals("MAP")).findFirst().orElseThrow();
    FilterCondition condition = new FilterCondition(mapProfile, new String[]{"val4"}, CompareFunction.EQUAL);
    CompositeFilter filter = new CompositeFilter(List.of(condition), LogicalOperator.AND);

    List<GanttColumnCount> actualMapMessage = dStore.getGanttCount(tableName,
                                                                   cProfiles.stream().filter(p -> p.getColName().equals("MAP")).findFirst().orElseThrow(),
                                                                   cProfiles.stream().filter(p -> p.getColName().equals("MESSAGE")).findFirst().orElseThrow(),
                                                                   filter, Integer.MIN_VALUE, Integer.MAX_VALUE);

    Map<String, Integer> expectedGantt = new HashMap<>();
    expectedGantt.put(testMessage2, 8);

    boolean foundVal4 = false;
    for (GanttColumnCount column : actualMapMessage) {
      if ("val4".equals(column.getKey())) {
        foundVal4 = true;
        assertEquals(expectedGantt, column.getGantt());
      }
    }
    assertTrue(foundVal4, "Should find val4 in gantt results");
  }

  @Test
  public void computeGanttWithArrayFilterTest() throws BeginEndWrongOrderException, SqlColMetadataException, GanttColumnNotSupportedException {
    CProfile arrayProfile = cProfiles.stream().filter(p -> p.getColName().equals("ARRAY")).findFirst().orElseThrow();
    FilterCondition condition = new FilterCondition(arrayProfile, new String[]{"array value 1"}, CompareFunction.EQUAL);
    CompositeFilter filter = new CompositeFilter(List.of(condition), LogicalOperator.AND);

    List<GanttColumnCount> actualMapMessage = dStore.getGanttCount(tableName,
                                                                   cProfiles.stream().filter(p -> p.getColName().equals("MAP")).findFirst().orElseThrow(),
                                                                   cProfiles.stream().filter(p -> p.getColName().equals("MESSAGE")).findFirst().orElseThrow(),
                                                                   filter, Integer.MIN_VALUE, Integer.MAX_VALUE);

    Map<String, Integer> expectedTotalCounts = new HashMap<>();
    expectedTotalCounts.put(testMessage1, 12);
    expectedTotalCounts.put(testMessage2, 30);
    expectedTotalCounts.put(testMessage3, 48);

    Map<String, Integer> actualTotalCounts = new HashMap<>();
    for (GanttColumnCount column : actualMapMessage) {
      column.getGantt().forEach((message, count) ->
                                    actualTotalCounts.merge(message, count, Integer::sum));
    }

    assertEquals(expectedTotalCounts, actualTotalCounts);
  }

  @Test
  public void computeGanttWithMultipleFiltersTest() throws BeginEndWrongOrderException, SqlColMetadataException, GanttColumnNotSupportedException {
    CProfile mapProfile = cProfiles.stream().filter(p -> p.getColName().equals("MAP")).findFirst().orElseThrow();
    CProfile messageProfile = cProfiles.stream().filter(p -> p.getColName().equals("MESSAGE")).findFirst().orElseThrow();

    FilterCondition mapCondition = new FilterCondition(mapProfile, new String[]{"val7"}, CompareFunction.EQUAL);
    FilterCondition messageCondition = new FilterCondition(messageProfile, new String[]{testMessage3}, CompareFunction.EQUAL);
    CompositeFilter filter = new CompositeFilter(List.of(mapCondition, messageCondition), LogicalOperator.AND);

    List<GanttColumnCount> actualMapMessage = dStore.getGanttCount(tableName,
                                                                   cProfiles.stream().filter(p -> p.getColName().equals("MAP")).findFirst().orElseThrow(),
                                                                   cProfiles.stream().filter(p -> p.getColName().equals("MESSAGE")).findFirst().orElseThrow(),
                                                                   filter, Integer.MIN_VALUE, Integer.MAX_VALUE);

    Map<String, Integer> expectedGantt = new HashMap<>();
    expectedGantt.put(testMessage3, 14);

    boolean foundVal7 = false;
    for (GanttColumnCount column : actualMapMessage) {
      if ("val7".equals(column.getKey())) {
        foundVal7 = true;
        assertEquals(expectedGantt, column.getGantt());
      }
    }
    assertTrue(foundVal7, "Should find val7 in gantt results");
  }
  @Test
  public void computeStackedTestWithLongColumn() throws BeginEndWrongOrderException, SqlColMetadataException {
    CProfile longProfile = cProfiles.stream().filter(p -> p.getColName().equals("LONG_COL")).findFirst().orElseThrow();

    List<StackedColumn> listCount = dStore.getStacked(tableName, longProfile, GroupFunction.COUNT, null,
                                                      Integer.MIN_VALUE, Integer.MAX_VALUE);

    assertEquals(3, listCount.size());

    for (StackedColumn column : listCount) {
      Map<String, Integer> keyCount = column.getKeyCount();
      if (keyCount.containsKey(String.valueOf(testLongValue1))) {
        assertEquals(2, keyCount.get(String.valueOf(testLongValue1)).intValue());
      } else if (keyCount.containsKey(String.valueOf(testLongValue2))) {
        assertEquals(2, keyCount.get(String.valueOf(testLongValue2)).intValue());
      } else if (keyCount.containsKey(String.valueOf(testLongValue3))) {
        assertEquals(2, keyCount.get(String.valueOf(testLongValue3)).intValue());
      }
    }

    List<StackedColumn> listSum = dStore.getStacked(tableName, longProfile, GroupFunction.SUM, null,
                                                    Integer.MIN_VALUE, Integer.MAX_VALUE);

    assertEquals(1, listSum.size());
    StackedColumn sumColumn = listSum.get(0);
    assertNotNull(sumColumn.getKeySum());

    double expectedSum = (testLongValue1 * 2) + (testLongValue2 * 2) + (testLongValue3 * 2);
    assertEquals(expectedSum, sumColumn.getKeySum().get("LONG_COL"), 0.001);

    List<StackedColumn> listAvg = dStore.getStacked(tableName, longProfile, GroupFunction.AVG, null,
                                                    Integer.MIN_VALUE, Integer.MAX_VALUE);

    assertEquals(1, listAvg.size());
    StackedColumn avgColumn = listAvg.get(0);
    assertNotNull(avgColumn.getKeyAvg());

    double expectedAvg = (testLongValue1 + testLongValue2 + testLongValue3) / 3.0;
    assertEquals(expectedAvg, avgColumn.getKeyAvg().get("LONG_COL"), 0.001);
  }

  @Test
  public void computeGanttTestWithLongColumn() throws BeginEndWrongOrderException, SqlColMetadataException, GanttColumnNotSupportedException {
    CProfile longProfile = cProfiles.stream().filter(p -> p.getColName().equals("LONG_COL")).findFirst().orElseThrow();
    CProfile messageProfile = cProfiles.stream().filter(p -> p.getColName().equals("MESSAGE")).findFirst().orElseThrow();

    List<GanttColumnCount> actualLongMessage = dStore.getGanttCount(tableName, longProfile, messageProfile,
                                                                    null, Integer.MIN_VALUE, Integer.MAX_VALUE);

    assertEquals(3, actualLongMessage.size());

    for (GanttColumnCount column : actualLongMessage) {
      String longValue = column.getKey();
      Map<String, Integer> ganttData = column.getGantt();

      if (String.valueOf(testLongValue1).equals(longValue)) {
        assertEquals(1, ganttData.size());
        assertEquals(2, ganttData.get(testMessage1).intValue());
      } else if (String.valueOf(testLongValue2).equals(longValue)) {
        assertEquals(1, ganttData.size());
        assertEquals(2, ganttData.get(testMessage2).intValue());
      } else if (String.valueOf(testLongValue3).equals(longValue)) {
        assertEquals(1, ganttData.size());
        assertEquals(2, ganttData.get(testMessage3).intValue());
      }
    }

    List<GanttColumnCount> actualMessageLong = dStore.getGanttCount(tableName, messageProfile, longProfile,
                                                                    null, Integer.MIN_VALUE, Integer.MAX_VALUE);

    assertEquals(3, actualMessageLong.size());

    for (GanttColumnCount column : actualMessageLong) {
      String message = column.getKey();
      Map<String, Integer> ganttData = column.getGantt();

      if (testMessage1.equals(message)) {
        assertEquals(1, ganttData.size());
        assertEquals(2, ganttData.get(String.valueOf(testLongValue1)).intValue());
      } else if (testMessage2.equals(message)) {
        assertEquals(1, ganttData.size());
        assertEquals(2, ganttData.get(String.valueOf(testLongValue2)).intValue());
      } else if (testMessage3.equals(message)) {
        assertEquals(1, ganttData.size());
        assertEquals(2, ganttData.get(String.valueOf(testLongValue3)).intValue());
      }
    }
  }

  @Test
  public void computeGanttSumWithLongColumn() throws BeginEndWrongOrderException, SqlColMetadataException, GanttColumnNotSupportedException {
    CProfile longProfile = cProfiles.stream().filter(p -> p.getColName().equals("LONG_COL")).findFirst().orElseThrow();
    CProfile messageProfile = cProfiles.stream().filter(p -> p.getColName().equals("MESSAGE")).findFirst().orElseThrow();

    List<GanttColumnSum> actualMessageLongSum = dStore.getGanttSum(tableName, messageProfile, longProfile,
                                                                   null, Integer.MIN_VALUE, Integer.MAX_VALUE);

    assertEquals(3, actualMessageLongSum.size());

    for (GanttColumnSum column : actualMessageLongSum) {
      String message = column.getKey();
      double sum = column.getValue();

      if (testMessage1.equals(message)) {
        assertEquals(testLongValue1 * 2, sum, 0.001);
      } else if (testMessage2.equals(message)) {
        assertEquals(testLongValue2 * 2, sum, 0.001);
      } else if (testMessage3.equals(message)) {
        assertEquals(testLongValue3 * 2, sum, 0.001);
      }
    }
  }

  @Test
  public void computeDistinctWithLongColumn() throws BeginEndWrongOrderException {
    CProfile longProfile = cProfiles.stream().filter(p -> p.getColName().equals("LONG_COL")).findFirst().orElseThrow();

    List<String> distinctLongs = dStore.getDistinct(tableName, longProfile, OrderBy.ASC, null,
                                                    10, Integer.MIN_VALUE, Integer.MAX_VALUE);

    assertEquals(3, distinctLongs.size());

    assertEquals(String.valueOf(testLongValue1), distinctLongs.get(0));
    assertEquals(String.valueOf(testLongValue2), distinctLongs.get(1));
    assertEquals(String.valueOf(testLongValue3), distinctLongs.get(2));

    List<String> distinctLongsDesc = dStore.getDistinct(tableName, longProfile, OrderBy.DESC, null,
                                                        10, Integer.MIN_VALUE, Integer.MAX_VALUE);

    assertEquals(3, distinctLongsDesc.size());
    assertEquals(String.valueOf(testLongValue3), distinctLongsDesc.get(0));
    assertEquals(String.valueOf(testLongValue2), distinctLongsDesc.get(1));
    assertEquals(String.valueOf(testLongValue1), distinctLongsDesc.get(2));
  }

  @Test
  public void computeStackedTestWithLongColumnAndFilter() throws BeginEndWrongOrderException, SqlColMetadataException {
    CProfile longProfile = cProfiles.stream().filter(p -> p.getColName().equals("LONG_COL")).findFirst().orElseThrow();
    CProfile messageProfile = cProfiles.stream().filter(p -> p.getColName().equals("MESSAGE")).findFirst().orElseThrow();

    FilterCondition condition = new FilterCondition(longProfile, new String[]{String.valueOf(testLongValue1)}, CompareFunction.EQUAL);
    CompositeFilter filter = new CompositeFilter(List.of(condition), LogicalOperator.AND);

    List<StackedColumn> listMessage = dStore.getStacked(tableName, messageProfile, GroupFunction.COUNT,
                                                        filter, Integer.MIN_VALUE, Integer.MAX_VALUE);

    assertEquals(1, listMessage.size());
    Map<String, Integer> counts = listMessage.get(0).getKeyCount();

    assertEquals(1, counts.size());
    assertEquals(2, counts.get(testMessage1).intValue());
    assertFalse(counts.containsKey(testMessage2));
    assertFalse(counts.containsKey(testMessage3));
  }

  @Test
  public void computeGanttTestWithLongColumnFilter() throws BeginEndWrongOrderException, SqlColMetadataException, GanttColumnNotSupportedException {
    CProfile longProfile = cProfiles.stream().filter(p -> p.getColName().equals("LONG_COL")).findFirst().orElseThrow();
    CProfile messageProfile = cProfiles.stream().filter(p -> p.getColName().equals("MESSAGE")).findFirst().orElseThrow();
    CProfile mapProfile = cProfiles.stream().filter(p -> p.getColName().equals("MAP")).findFirst().orElseThrow();

    FilterCondition condition = new FilterCondition(longProfile, new String[]{String.valueOf(testLongValue2)}, CompareFunction.EQUAL);
    CompositeFilter filter = new CompositeFilter(List.of(condition), LogicalOperator.AND);

    List<GanttColumnCount> actualMapMessage = dStore.getGanttCount(tableName, mapProfile, messageProfile,
                                                                   filter, Integer.MIN_VALUE, Integer.MAX_VALUE);

    Map<String, Integer> expectedCounts = new HashMap<>();
    expectedCounts.put("val4", 8);
    expectedCounts.put("val5", 10);
    expectedCounts.put("val6", 12);

    Map<String, Integer> actualTotalCounts = new HashMap<>();
    for (GanttColumnCount column : actualMapMessage) {
      column.getGantt().forEach((message, count) -> {
        if (testMessage2.equals(message)) {
          actualTotalCounts.merge(column.getKey(), count, Integer::sum);
        }
      });
    }

    assertEquals(expectedCounts, actualTotalCounts);
  }

  @Test
  public void computeDistinctWithMapFilterTest() throws BeginEndWrongOrderException {
    CProfile mapProfile = cProfiles.stream().filter(p -> p.getColName().equals("MAP")).findFirst().orElseThrow();
    FilterCondition condition = new FilterCondition(mapProfile, new String[]{"val4"}, CompareFunction.EQUAL);
    CompositeFilter filter = new CompositeFilter(List.of(condition), LogicalOperator.AND);

    List<String> distinctMessages = dStore.getDistinct(tableName,
                                                       cProfiles.stream().filter(p -> p.getColName().equals("MESSAGE")).findFirst().orElseThrow(),
                                                       OrderBy.ASC, filter, 10, Integer.MIN_VALUE, Integer.MAX_VALUE);

    assertEquals(1, distinctMessages.size());
    assertEquals(testMessage2, distinctMessages.get(0));
  }

  @Test
  public void computeDistinctWithArrayFilterTest() throws BeginEndWrongOrderException {
    CProfile arrayProfile = cProfiles.stream().filter(p -> p.getColName().equals("ARRAY")).findFirst().orElseThrow();
    FilterCondition condition = new FilterCondition(arrayProfile, new String[]{"array value 1"}, CompareFunction.EQUAL);
    CompositeFilter filter = new CompositeFilter(List.of(condition), LogicalOperator.AND);

    List<String> distinctMessages = dStore.getDistinct(tableName,
                                                       cProfiles.stream().filter(p -> p.getColName().equals("MESSAGE")).findFirst().orElseThrow(),
                                                       OrderBy.ASC, filter, 10, Integer.MIN_VALUE, Integer.MAX_VALUE);

    assertEquals(3, distinctMessages.size());
    assertTrue(distinctMessages.contains(testMessage1));
    assertTrue(distinctMessages.contains(testMessage2));
    assertTrue(distinctMessages.contains(testMessage3));
  }

  @Test
  public void computeDistinctMapWithFilterTest() throws BeginEndWrongOrderException {
    CProfile messageProfile = cProfiles.stream().filter(p -> p.getColName().equals("MESSAGE")).findFirst().orElseThrow();
    FilterCondition condition = new FilterCondition(messageProfile, new String[]{testMessage1}, CompareFunction.EQUAL);
    CompositeFilter filter = new CompositeFilter(List.of(condition), LogicalOperator.AND);

    List<String> distinctMapKeys = dStore.getDistinct(tableName,
                                                      cProfiles.stream().filter(p -> p.getColName().equals("MAP")).findFirst().orElseThrow(),
                                                      OrderBy.ASC, filter, 10, Integer.MIN_VALUE, Integer.MAX_VALUE);

    assertEquals(3, distinctMapKeys.size());
    assertTrue(distinctMapKeys.contains("val1"));
    assertTrue(distinctMapKeys.contains("val2"));
    assertTrue(distinctMapKeys.contains("val3"));
    assertFalse(distinctMapKeys.contains("val4"));
  }

  @Test
  public void computeDistinctArrayWithFilterTest() throws BeginEndWrongOrderException {
    CProfile messageProfile = cProfiles.stream().filter(p -> p.getColName().equals("MESSAGE")).findFirst().orElseThrow();
    FilterCondition condition = new FilterCondition(messageProfile, new String[]{testMessage2}, CompareFunction.EQUAL);
    CompositeFilter filter = new CompositeFilter(List.of(condition), LogicalOperator.AND);

    List<String> distinctArrayValues = dStore.getDistinct(tableName,
                                                          cProfiles.stream().filter(p -> p.getColName().equals("ARRAY")).findFirst().orElseThrow(),
                                                          OrderBy.ASC, filter, 10, Integer.MIN_VALUE, Integer.MAX_VALUE);

    assertEquals(2, distinctArrayValues.size());
    assertTrue(distinctArrayValues.contains("array value 1"));
    assertTrue(distinctArrayValues.contains("array value 2"));
  }

  @Test
  public void computeDistinctWithMultipleFiltersTest() throws BeginEndWrongOrderException {
    CProfile mapProfile = cProfiles.stream().filter(p -> p.getColName().equals("MAP")).findFirst().orElseThrow();
    CProfile messageProfile = cProfiles.stream().filter(p -> p.getColName().equals("MESSAGE")).findFirst().orElseThrow();

    FilterCondition mapCondition = new FilterCondition(mapProfile, new String[]{"val7"}, CompareFunction.EQUAL);
    FilterCondition messageCondition = new FilterCondition(messageProfile, new String[]{testMessage3}, CompareFunction.EQUAL);
    CompositeFilter filter = new CompositeFilter(List.of(mapCondition, messageCondition), LogicalOperator.AND);

    List<String> distinctArrayValues = dStore.getDistinct(tableName,
                                                          cProfiles.stream().filter(p -> p.getColName().equals("ARRAY")).findFirst().orElseThrow(),
                                                          OrderBy.ASC, filter, 10, Integer.MIN_VALUE, Integer.MAX_VALUE);

    assertEquals(2, distinctArrayValues.size());
    assertTrue(distinctArrayValues.contains("array value 1"));
    assertTrue(distinctArrayValues.contains("array value 2"));
  }

  @Test
  public void computeRawTest() {
    List<List<Object>> expected01 = transpose(data01);
    List<List<Object>> expected02 = transpose(data02);
    List<List<Object>> expected03 = transpose(data03);
    expected01.addAll(expected02);
    expected01.addAll(expected03);

    List<List<Object>> actual = getRawDataAll(Integer.MIN_VALUE, Integer.MAX_VALUE);

    String expected = expected01.stream()
        .map(innerList -> innerList.stream()
            .map(obj -> obj instanceof String[] ?
                Arrays.toString((String[]) obj) :
                obj instanceof Object[] ?
                    Arrays.deepToString((Object[]) obj) :
                    obj.toString())
            .collect(Collectors.joining(", ")))
        .collect(Collectors.joining("], [", "[", "]"));

    assertEquals("[" + expected + "]", String.valueOf(actual));
  }

  private List<GanttColumnCount> getTestDataMapMessage() {
    List<GanttColumnCount> expected = new ArrayList<>();
    Map<String, Integer> ganttData;

    ganttData = new HashMap<>();
    ganttData.put("Test message 3", 18);
    expected.add(new GanttColumnCount("val9", ganttData));

    ganttData = new HashMap<>();
    ganttData.put("Test message 3", 16);
    expected.add(new GanttColumnCount("val8", ganttData));

    ganttData = new HashMap<>();
    ganttData.put("Test message 3", 14);
    expected.add(new GanttColumnCount("val7", ganttData));

    ganttData = new HashMap<>();
    ganttData.put("Test message 2", 12);
    expected.add(new GanttColumnCount("val6", ganttData));

    ganttData = new HashMap<>();
    ganttData.put("Test message 2", 10);
    expected.add(new GanttColumnCount("val5", ganttData));

    ganttData = new HashMap<>();
    ganttData.put("Test message 2", 8);
    expected.add(new GanttColumnCount("val4", ganttData));

    ganttData = new HashMap<>();
    ganttData.put("Test message 1", 6);
    expected.add(new GanttColumnCount("val3", ganttData));

    ganttData = new HashMap<>();
    ganttData.put("Test message 1", 4);
    expected.add(new GanttColumnCount("val2", ganttData));

    ganttData = new HashMap<>();
    ganttData.put("Test message 1", 2);
    expected.add(new GanttColumnCount("val1", ganttData));

    return expected;
  }

  private List<GanttColumnCount> getTestArrayMap() {
    List<GanttColumnCount> expected = new ArrayList<>();

    Map<String, Integer> array1Gantt = new HashMap<>();
    array1Gantt.put("val1", 2);
    array1Gantt.put("val2", 4);
    array1Gantt.put("val3", 6);
    array1Gantt.put("val4", 8);
    array1Gantt.put("val5", 10);
    array1Gantt.put("val6", 12);
    array1Gantt.put("val7", 14);
    array1Gantt.put("val8", 16);
    array1Gantt.put("val9", 18);

    expected.add(new GanttColumnCount("array value 1", array1Gantt));
    expected.add(new GanttColumnCount("array value 2", array1Gantt));

    return expected;
  }
}