package ru.dimension.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.dimension.db.common.AbstractDirectTest;
import ru.dimension.db.exception.BeginEndWrongOrderException;
import ru.dimension.db.exception.EnumByteExceedException;
import ru.dimension.db.exception.SqlColMetadataException;
import ru.dimension.db.exception.TableNameEmptyException;
import ru.dimension.db.metadata.DataType;
import ru.dimension.db.model.GranularityFunction;
import ru.dimension.db.model.GroupFunction;
import ru.dimension.db.model.PercentileFunction;
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

public class DBase15PercentileTest extends AbstractDirectTest {

  private static final long MINUTE_MS = 60_000L;
  private static final long HOUR_MS = 3_600_000L;

  private long rangeBegin;
  private long rangeEnd;

  @BeforeAll
  public void init() {
    SProfile sProfile = new SProfile();
    sProfile.setTableName(tableName);
    sProfile.setTableType(TType.TIME_SERIES);
    sProfile.setIndexType(IType.GLOBAL);
    sProfile.setAnalyzeType(AType.ON_LOAD);
    sProfile.setBackendType(BType.BERKLEYDB);
    sProfile.setCompression(false);

    Map<String, CSType> csTypeMap = new LinkedHashMap<>();
    csTypeMap.put("ID", CSType.builder().isTimeStamp(true).sType(SType.RAW).cType(CType.LONG).dType(DataType.LONG).build());
    csTypeMap.put("LONG_FIELD", CSType.builder().isTimeStamp(false).sType(SType.RAW).cType(CType.LONG).dType(DataType.LONG).build());
    csTypeMap.put("STRING_FIELD", CSType.builder().isTimeStamp(false).sType(SType.RAW).cType(CType.STRING).dType(DataType.STRING).build());

    sProfile.setCsTypeMap(csTypeMap);

    putPercentileTestData(sProfile);
  }

  /**
   * Inserts 10 rows, one per minute starting at startTime.
   * LONG_FIELD values: 1..10 (ascending)
   * STRING_FIELD values: alternating "A", "B" (5 A's, 5 B's).
   */
  private void putPercentileTestData(SProfile sProfile) {
    try {
      var tProfile = dStore.loadDirectTableMetadata(sProfile);
      cProfiles = tProfile.getCProfiles();

      rangeBegin = startTime;
      rangeEnd = startTime + 10 * MINUTE_MS;

      for (int i = 0; i < 10; i++) {
        long ts = startTime + i * MINUTE_MS;
        long longVal = i + 1L;
        String strVal = (i % 2 == 0) ? "A" : "B";

        List<List<Object>> data = new ArrayList<>();
        data.add(0, listOf(ts));
        data.add(1, listOf(longVal));
        data.add(2, listOf(strVal));

        dStore.putDataDirect(tProfile.getTableName(), data);
      }
    } catch (SqlColMetadataException | EnumByteExceedException | TableNameEmptyException e) {
      throw new RuntimeException(e);
    }
  }

  private static <T> ArrayList<Object> listOf(T value) {
    ArrayList<Object> list = new ArrayList<>(1);
    list.add(value);
    return list;
  }

  private CProfile profile(String colName) {
    return cProfiles.stream()
        .filter(p -> p.getColName().equalsIgnoreCase(colName))
        .findAny()
        .orElseThrow();
  }

  @Test
  public void avgWithP50ReturnsMedian() throws BeginEndWrongOrderException, SqlColMetadataException {
    List<StackedColumn> result = dStore.getStacked(tableName,
                                                   profile("LONG_FIELD"),
                                                   GroupFunction.AVG,
                                                   PercentileFunction.P50,
                                                   GranularityFunction.AUTO,
                                                   null,
                                                   rangeBegin, rangeEnd);

    assertEquals(1, result.size());
    Map<String, Double> percentile = result.getFirst().getKeyPercentile();
    assertEquals(5.0, percentile.get("LONG_FIELD"));
  }

  @Test
  public void avgWithP90ReturnsExpected() throws BeginEndWrongOrderException, SqlColMetadataException {
    List<StackedColumn> result = dStore.getStacked(tableName,
                                                   profile("LONG_FIELD"),
                                                   GroupFunction.AVG,
                                                   PercentileFunction.P90,
                                                   GranularityFunction.AUTO,
                                                   null,
                                                   rangeBegin, rangeEnd);

    assertEquals(9.0, result.getFirst().getKeyPercentile().get("LONG_FIELD"));
  }

  @Test
  public void sumWithP95ReturnsMaxValueForControlledInput() throws BeginEndWrongOrderException, SqlColMetadataException {
    List<StackedColumn> result = dStore.getStacked(tableName,
                                                   profile("LONG_FIELD"),
                                                   GroupFunction.SUM,
                                                   PercentileFunction.P95,
                                                   GranularityFunction.AUTO,
                                                   null,
                                                   rangeBegin, rangeEnd);

    assertEquals(10.0, result.getFirst().getKeyPercentile().get("LONG_FIELD"));
  }

  @Test
  public void countWithP50AndMinuteGranularityProducesPercentilePerGroup()
      throws BeginEndWrongOrderException, SqlColMetadataException {
    List<StackedColumn> result = dStore.getStacked(tableName,
                                                   profile("STRING_FIELD"),
                                                   GroupFunction.COUNT,
                                                   PercentileFunction.P50,
                                                   GranularityFunction.MINUTE,
                                                   null,
                                                   rangeBegin, rangeEnd);

    assertEquals(1, result.size());
    Map<String, Double> percentile = result.getFirst().getKeyPercentile();
    assertNotNull(percentile);
    assertTrue(percentile.containsKey("A"));
    assertTrue(percentile.containsKey("B"));
    assertEquals(1.0, percentile.get("A"));
    assertEquals(1.0, percentile.get("B"));
  }

  @Test
  public void countWithP90AndHourGranularityFallsBackForTooFewBuckets()
      throws BeginEndWrongOrderException, SqlColMetadataException {
    List<StackedColumn> result = dStore.getStacked(tableName,
                                                   profile("STRING_FIELD"),
                                                   GroupFunction.COUNT,
                                                   PercentileFunction.P90,
                                                   GranularityFunction.HOUR,
                                                   null,
                                                   rangeBegin, rangeEnd);

    assertTrue(result.stream()
                   .anyMatch(c -> !c.getKeyCount().isEmpty()),
               "Expected fallback to populate keyCount when buckets < 5");
    assertTrue(result.stream()
                   .allMatch(c -> c.getKeyPercentile() == null || c.getKeyPercentile().isEmpty()),
               "Expected keyPercentile to remain empty in fallback path");
  }

  @Test
  public void noneDelegatesToRegularGetStacked() throws BeginEndWrongOrderException, SqlColMetadataException {
    List<StackedColumn> percentileNone = dStore.getStacked(tableName,
                                                           profile("STRING_FIELD"),
                                                           GroupFunction.COUNT,
                                                           PercentileFunction.NONE,
                                                           GranularityFunction.AUTO,
                                                           null,
                                                           rangeBegin, rangeEnd);

    List<StackedColumn> regular = dStore.getStacked(tableName,
                                                    profile("STRING_FIELD"),
                                                    GroupFunction.COUNT,
                                                    null,
                                                    rangeBegin, rangeEnd);

    assertEquals(regular.size(), percentileNone.size());
    for (int i = 0; i < regular.size(); i++) {
      assertEquals(regular.get(i).getKeyCount(), percentileNone.get(i).getKeyCount());
    }
  }

  @Test
  public void autoResolvesToMinuteForShortRange() {
    GranularityFunction resolved = GranularityFunction.resolve(GranularityFunction.AUTO,
                                                                rangeBegin,
                                                                rangeBegin + 30 * MINUTE_MS);
    assertEquals(GranularityFunction.MINUTE, resolved);
  }

  @Test
  public void autoResolvesToHourForOneDayRange() {
    GranularityFunction resolved = GranularityFunction.resolve(GranularityFunction.AUTO,
                                                                rangeBegin,
                                                                rangeBegin + 24 * HOUR_MS);
    assertEquals(GranularityFunction.HOUR, resolved);
  }

  @Test
  public void autoResolvesToDayForOneWeekRange() {
    GranularityFunction resolved = GranularityFunction.resolve(GranularityFunction.AUTO,
                                                                rangeBegin,
                                                                rangeBegin + 7 * 24 * HOUR_MS);
    assertEquals(GranularityFunction.DAY, resolved);
  }

  @Test
  public void percentileFunctionGetValueReturnsExpectedFractions() {
    assertEquals(0.50, PercentileFunction.P50.getValue());
    assertEquals(0.90, PercentileFunction.P90.getValue());
    assertEquals(0.95, PercentileFunction.P95.getValue());
    assertEquals(0.99, PercentileFunction.P99.getValue());
    assertEquals(0.0, PercentileFunction.NONE.getValue());
  }
}
