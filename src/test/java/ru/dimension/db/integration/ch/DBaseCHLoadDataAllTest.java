package ru.dimension.db.integration.ch;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import lombok.extern.log4j.Log4j2;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import ru.dimension.db.DBase;
import ru.dimension.db.backend.BerkleyDB;
import ru.dimension.db.config.DBaseConfig;
import ru.dimension.db.core.DStore;
import ru.dimension.db.exception.BeginEndWrongOrderException;
import ru.dimension.db.exception.GanttColumnNotSupportedException;
import ru.dimension.db.exception.SqlColMetadataException;
import ru.dimension.db.model.GroupFunction;
import ru.dimension.db.model.filter.CompositeFilter;
import ru.dimension.db.model.filter.FilterCondition;
import ru.dimension.db.model.filter.LogicalOperator;
import ru.dimension.db.model.CompareFunction;
import ru.dimension.db.model.output.GanttColumnCount;
import ru.dimension.db.model.output.GanttColumnSum;
import ru.dimension.db.model.output.StackedColumn;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.TProfile;
import ru.dimension.db.model.profile.table.AType;
import ru.dimension.db.model.profile.table.IType;
import ru.dimension.db.model.profile.table.TType;
import ru.dimension.db.source.ClickHouse;
import ru.dimension.db.source.ClickHouseDatabase;
import ru.dimension.db.source.ClickHouseDatabase.Step;

@Log4j2
@Disabled
public class DBaseCHLoadDataAllTest implements ClickHouse {

  private static final String url = "jdbc:clickhouse://localhost:8123/?user=admin&password=admin";

  private static final StringBuilder markdownTableByProfile = new StringBuilder();
  private static final StringBuilder markdownTableByGanttCount = new StringBuilder();
  private static final StringBuilder markdownTableByGanttSum = new StringBuilder();
  private static final StringBuilder markdownTableByStacked = new StringBuilder();
  private static final List<Map<String, String>> profileResults = new ArrayList<>();

  private static final String NL = System.lineSeparator();
  private static final int BATCH_SIZE = 2;

  private static final long EXACT_ROW_COUNT = 78325655;
  private static final double EXACT_DATA_MB = 9880;

  private static final List<String> profileKeys = new ArrayList<>();

  private static final Map<String, List<String>> testResultsCount = new LinkedHashMap<>();
  private static final Map<String, List<String>> testResultsSum = new LinkedHashMap<>();
  private static final Map<String, List<String>> testResultsStacked = new LinkedHashMap<>();

  private static final List<GanttCountTestDefinition> ganttCountTestDefinitions = Arrays.asList(
      new GanttCountTestDefinition("getGanttRawRaw", "PICKUP_CDELIGIBIL", "VENDOR_ID", "pickup_cdeligibil__vendor_id.json"),
      new GanttCountTestDefinition("getGanttEnumEnum", "DROPOFF_PUMA", "DROPOFF_BOROCODE", "dropoff_puma__dropoff_borocode.json"),
      new GanttCountTestDefinition("getGanttHistHist", "TRIP_TYPE", "PICKUP_BORONAME", "trip_type__pickup_boroname.json"),
      new GanttCountTestDefinition("getGanttHistRaw", "TRIP_TYPE", "VENDOR_ID", "trip_type__vendor_id.json"),
      new GanttCountTestDefinition("getGanttHistEnum", "TRIP_TYPE", "DROPOFF_BORONAME", "trip_type__dropoff_boroname.json"),
      new GanttCountTestDefinition("getGanttEnumRaw", "DROPOFF_BORONAME", "VENDOR_ID", "dropoff_boroname__vendor_id.json"),
      new GanttCountTestDefinition("getGanttEnumHist", "DROPOFF_BORONAME", "PICKUP_BORONAME", "dropoff_boroname__pickup_boroname.json"),
      new GanttCountTestDefinition("getGanttRawHist", "PICKUP_CDELIGIBIL", "PICKUP_BORONAME", "pickup_cdeligibil__pickup_boroname.json"),
      new GanttCountTestDefinition("getGanttRawEnum", "PICKUP_CDELIGIBIL", "CAB_TYPE", "pickup_cdeligibil__cab_type.json")
  );

  private static final List<GanttSumTestDefinition> ganttSumTestDefinitions = Arrays.asList(
      new GanttSumTestDefinition("getGanttSumHistRaw", "TRIP_TYPE", "FARE_AMOUNT", "trip_type__fare_amount.json", null),
      new GanttSumTestDefinition("getGanttSumEnumRaw", "PICKUP_BORONAME", "TRIP_DISTANCE", "pickup_boroname__trip_distance.json", null),
      new GanttSumTestDefinition("getGanttSumRawRaw", "VENDOR_ID", "TOTAL_AMOUNT", "vendor_id__total_amount.json", null),
      new GanttSumTestDefinition("getGanttSumHistHist", "TRIP_TYPE", "PASSENGER_COUNT", "trip_type__passenger_count.json", null),
      new GanttSumTestDefinition("getGanttSumEnumEnum", "DROPOFF_BORONAME", "DROPOFF_BOROCODE", "dropoff_boroname__dropoff_borocode.json", null),
      new GanttSumTestDefinition("getGanttSumRawEnum", "PICKUP_CDELIGIBIL", "PICKUP_BOROCODE", "pickup_cdeligibil__pickup_borocode.json", null),
      new GanttSumTestDefinition("getGanttSumHistRawWithFilter", "TRIP_TYPE", "FARE_AMOUNT", "trip_type__fare_amount_sum_filtered.json",
                                 new FilterSpec("TRIP_TYPE", new String[]{"1", "2"}, CompareFunction.EQUAL))
  );

  private static final List<StackedTestDefinition> stackedTestDefinitions = Arrays.asList(
      new StackedTestDefinition("stackedHist", "TRIP_TYPE", false, null, "trip_type.json"),
      new StackedTestDefinition("stackedHistDate", "TRIP_TYPE", true, LocalDate.of(2016, 1, 1), "trip_type_date.json"),
      new StackedTestDefinition("stackedEnum", "DROPOFF_BORONAME", false, null, "dropoff_boroname.json"),
      new StackedTestDefinition("stackedEnumDate", "DROPOFF_BORONAME", true, LocalDate.of(2016, 1, 1), "dropoff_boroname_date.json"),
      new StackedTestDefinition("stackedRaw", "VENDOR_ID", false, null, "vendor_id.json"),
      new StackedTestDefinition("stackedRawDate", "VENDOR_ID", true, LocalDate.of(2016, 1, 1), "vendor_id_date.json")
  );

  static {
    // Initialize results maps
    for (GanttCountTestDefinition def : ganttCountTestDefinitions) {
      testResultsCount.put(def.testName, new ArrayList<>());
    }
    for (GanttSumTestDefinition def : ganttSumTestDefinitions) {
      testResultsSum.put(def.testName, new ArrayList<>());
    }
    for (StackedTestDefinition def : stackedTestDefinitions) {
      testResultsStacked.put(def.testName, new ArrayList<>());
    }
  }

  private static class TestResources {
    final Path databaseDir;
    final BerkleyDB berkleyDB;
    final DBase dBase;
    final DStore dStore;
    final ClickHouseDatabase clickHouseDB;

    TestResources(Path databaseDir) throws SQLException, IOException {
      this.databaseDir = databaseDir;
      String dbFolder = databaseDir.toString();
      log.info("Creating database in temporary folder: {}", dbFolder);

      this.berkleyDB = new BerkleyDB(dbFolder, true);
      this.clickHouseDB = new ClickHouseDatabase(url);

      DBaseConfig dBaseConfig = new DBaseConfig().setConfigDirectory(dbFolder);
      this.dBase = new DBase(dBaseConfig, berkleyDB.getStore());
      this.dStore = dBase.getDStore();
    }

    void close() throws SQLException {
      if (clickHouseDB != null) clickHouseDB.close();
      if (berkleyDB != null) berkleyDB.closeDatabase();
    }
  }

  @ParameterizedTest(name = "TType={0}, IType={1}, AType={2}, compression={3}")
  @MethodSource("loadParameters")
  public void testLoadDataByHour(
      TType tType,
      IType iType,
      AType aType,
      boolean compression,
      @TempDir Path databaseDir) throws SQLException, IOException {

    TestResources resources = new TestResources(databaseDir);
    try {
      Instant start = Instant.now();
      runLoadTest(resources, tType, iType, aType, compression);
      Duration loadDuration = Duration.between(start, Instant.now());
      long loadMinutes = loadDuration.toMinutes();
      long loadSecondsPart = loadDuration.minusMinutes(loadMinutes).getSeconds();
      String loadTimeFormatted = String.format("%d min %d sec", loadMinutes, loadSecondsPart);

      double totalSeconds = loadDuration.toMillis() / 1000.0;
      long avgRowsPerSec = (totalSeconds > 0) ? (long) (EXACT_ROW_COUNT / totalSeconds) : 0;
      double avgMBPerSec = (totalSeconds > 0) ? (EXACT_DATA_MB) / totalSeconds : 0.0;

      long sizeBytes = calculateFolderSize(resources.databaseDir);
      double sizeGB = sizeBytes / (1024.0 * 1024.0 * 1024.0);

      Map<String, String> results = new LinkedHashMap<>();
      results.put("TType", tType.toString());
      results.put("IType", iType.toString());
      results.put("AType", aType.toString());
      results.put("Compression", String.valueOf(compression));
      results.put("Load (min)", loadTimeFormatted);
      results.put("Size (GB)", String.format("%.3f", sizeGB));
      results.put("Avg Rows/Sec", String.format("%,d", avgRowsPerSec));
      results.put("Avg MB/Sec", String.format("%.2f", avgMBPerSec));
      profileResults.add(results);

      // Run all tests by profileKey
      String profileKey = String.format("%s_%s_%s_%s", tType, iType, aType, compression);
      profileKeys.add(profileKey);

      runAllStackedTests(resources, profileKey);
      runAllGanttCountTests(resources, profileKey);
      runAllGanttSumTests(resources, profileKey);
    } finally {
      resources.close();
    }
  }

  private void runLoadTest(TestResources resources,
                           TType tType,
                           IType iType,
                           AType aType,
                           boolean compression) {
    try {
      List<CProfile> cProfiles = resources.clickHouseDB.loadDataJdbc(
          ClickHouse.select2016,
          resources.dStore,
          tType,
          iType,
          aType,
          compression,
          20000,
          LocalDate.of(2016, 1, 1),
          LocalDate.of(2017, 1, 1),
          Step.DAY
      );
      assertTrue(cProfiles.size() > 0, "Should load some column profiles");
    } catch (Exception e) {
      log.error("Test failed for parameters: {} {} {} {}", tType, iType, aType, compression, e);
      fail("Exception occurred during test execution", e);
    }
  }

  static Stream<Arguments> loadParameters() {
    return Stream.of(
        Arguments.of(TType.TIME_SERIES, IType.GLOBAL, AType.ON_LOAD, true),
        Arguments.of(TType.TIME_SERIES, IType.LOCAL, AType.ON_LOAD, true),
        Arguments.of(TType.TIME_SERIES, IType.LOCAL, AType.FULL_PASS_ONCE, true),
        Arguments.of(TType.TIME_SERIES, IType.LOCAL, AType.FULL_PASS_EACH, true)
    );
  }

  private void runAllGanttCountTests(TestResources resources, String profileKey) {
    try {
      TProfile tProfile = resources.dStore.getTProfile(ClickHouse.tableName);
      List<CProfile> cProfiles = tProfile.getCProfiles();
      ObjectMapper objectMapper = new ObjectMapper();

      for (GanttCountTestDefinition testDef : ganttCountTestDefinitions) {
        String result = runGanttCountTest(resources, testDef, tProfile, cProfiles, objectMapper);
        testResultsCount.get(testDef.testName).add(result);
      }
    } catch (Exception e) {
      log.error("Failed to run gantt COUNT tests for profile {}", profileKey, e);
    }
  }

  private void runAllGanttSumTests(TestResources resources, String profileKey) {
    try {
      TProfile tProfile = resources.dStore.getTProfile(ClickHouse.tableName);
      List<CProfile> cProfiles = tProfile.getCProfiles();
      ObjectMapper objectMapper = new ObjectMapper();

      for (GanttSumTestDefinition testDef : ganttSumTestDefinitions) {
        String result = runGanttSumTest(resources, testDef, tProfile, cProfiles, objectMapper);
        testResultsSum.get(testDef.testName).add(result);
      }
    } catch (Exception e) {
      log.error("Failed to run gantt SUM tests for profile {}", profileKey, e);
    }
  }

  private void runAllStackedTests(TestResources resources, String profileKey) {
    try {
      TProfile tProfile = resources.dStore.getTProfile(ClickHouse.tableName);
      List<CProfile> cProfiles = tProfile.getCProfiles();
      ObjectMapper objectMapper = new ObjectMapper();

      for (StackedTestDefinition testDef : stackedTestDefinitions) {
        String result = runStackedTest(resources, testDef, tProfile, cProfiles, objectMapper);
        testResultsStacked.get(testDef.testName).add(result);
      }
    } catch (Exception e) {
      log.error("Failed to run STACKED tests for profile {}", profileKey, e);
    }
  }

  // ========== Gantt COUNT ==========
  private String runGanttCountTest(TestResources resources,
                                   GanttCountTestDefinition testDef,
                                   TProfile tProfile,
                                   List<CProfile> cProfiles,
                                   ObjectMapper objectMapper) {
    try {
      // Load expected results
      List<GanttColumnCount> expected = objectMapper.readValue(
          getGanttCountTestData(testDef.expectedJsonFileName),
          new TypeReference<>() {}
      );

      Instant singleStart = Instant.now();
      List<GanttColumnCount> actualSingle = getGanttCountActual(
          resources.dStore, tProfile, cProfiles,
          testDef.firstCol, testDef.secondCol
      );
      double singleTime = Duration.between(singleStart, Instant.now()).toMillis() / 1000.0;

      // Parallel execution
      Instant parallelStart = Instant.now();
      List<GanttColumnCount> actualParallel = getGanttCountActual(
          resources.dStore, tProfile, cProfiles,
          testDef.firstCol, testDef.secondCol, BATCH_SIZE
      );
      double parallelTime = Duration.between(parallelStart, Instant.now()).toMillis() / 1000.0;

      // Validate results
      assertGanttCountListEquals(expected, actualSingle);
      assertGanttCountMapEquals(expected, actualSingle);
      assertGanttCountListEquals(expected, actualParallel);
      assertGanttCountMapEquals(expected, actualParallel);

      return String.format("%.1f / %.1f", singleTime, parallelTime);
    } catch (Exception e) {
      log.error("Gantt COUNT test failed: {}", testDef.testName, e);
      return "ERROR";
    }
  }

  private void assertGanttCountMapEquals(List<GanttColumnCount> expected, List<GanttColumnCount> actual) {
    expected.forEach(exp ->
                         Assertions.assertEquals(
                             exp.getGantt(),
                             actual.stream()
                                 .filter(a -> a.getKey().equalsIgnoreCase(exp.getKey()))
                                 .findFirst()
                                 .orElseThrow()
                                 .getGantt()
                         )
    );
  }

  private void assertGanttCountListEquals(List<GanttColumnCount> expected, List<GanttColumnCount> actual) {
    assertTrue(
        expected.size() == actual.size() &&
            expected.containsAll(actual) &&
            actual.containsAll(expected)
    );
  }

  private String getGanttCountTestData(String fileName) throws IOException {
    return Files.readString(Paths.get("src", "test", "resources", "json", "gantt", fileName));
  }

  private List<GanttColumnCount> getGanttCountActual(
      DStore dStore, TProfile tProfile, List<CProfile> cProfiles,
      String firstColName, String secondColName
  ) throws Exception {
    long first = dStore.getFirst(tProfile.getTableName(), Long.MIN_VALUE, Long.MAX_VALUE);
    long last = dStore.getLast(tProfile.getTableName(), Long.MIN_VALUE, Long.MAX_VALUE);
    return getGanttCountActual(dStore, tProfile, cProfiles, firstColName, secondColName, first, last);
  }

  private List<GanttColumnCount> getGanttCountActual(
      DStore dStore, TProfile tProfile, List<CProfile> cProfiles,
      String firstColName, String secondColName, int batchSize
  ) throws Exception {
    long first = dStore.getFirst(tProfile.getTableName(), Long.MIN_VALUE, Long.MAX_VALUE);
    long last = dStore.getLast(tProfile.getTableName(), Long.MIN_VALUE, Long.MAX_VALUE);
    return getGanttCountActual(dStore, tProfile, cProfiles, firstColName, secondColName, batchSize, first, last);
  }

  private List<GanttColumnCount> getGanttCountActual(
      DStore dStore, TProfile tProfile, List<CProfile> cProfiles,
      String firstColName, String secondColName, long begin, long end
  ) throws Exception {
    CProfile firstLevel = findCProfile(cProfiles, firstColName);
    CProfile secondLevel = findCProfile(cProfiles, secondColName);
    return dStore.getGanttCount(tProfile.getTableName(), firstLevel, secondLevel, null, begin, end);
  }

  private List<GanttColumnCount> getGanttCountActual(
      DStore dStore, TProfile tProfile, List<CProfile> cProfiles,
      String firstColName, String secondColName, int batchSize, long begin, long end
  ) throws Exception {
    CProfile firstLevel = findCProfile(cProfiles, firstColName);
    CProfile secondLevel = findCProfile(cProfiles, secondColName);
    return dStore.getGanttCount(tProfile.getTableName(), firstLevel, secondLevel, null, batchSize, begin, end);
  }

  // ========== Gantt SUM ==========
  private String runGanttSumTest(TestResources resources,
                                 GanttSumTestDefinition testDef,
                                 TProfile tProfile,
                                 List<CProfile> cProfiles,
                                 ObjectMapper objectMapper) {
    try {
      List<GanttColumnSum> expected = objectMapper.readValue(
          getGanttSumTestData(testDef.expectedJsonFileName),
          new TypeReference<>() {}
      );

      Instant start = Instant.now();
      List<GanttColumnSum> actual;
      if (testDef.filter != null) {
        CompositeFilter filter = toCompositeFilter(cProfiles, testDef.filter);
        actual = getGanttSumActual(resources.dStore, tProfile, cProfiles, testDef.firstCol, testDef.secondCol, filter);
      } else {
        actual = getGanttSumActual(resources.dStore, tProfile, cProfiles, testDef.firstCol, testDef.secondCol);
      }
      double seconds = Duration.between(start, Instant.now()).toMillis() / 1000.0;

      assertGanttSumListEquals(expected, actual);
      return String.format("%.1f", seconds);
    } catch (Exception e) {
      log.error("Gantt SUM test failed: {}", testDef.testName, e);
      return "ERROR";
    }
  }

  private void assertGanttSumListEquals(List<GanttColumnSum> expected, List<GanttColumnSum> actual) {
    Assertions.assertEquals(expected.size(), actual.size(), "Lists should have same size");

    Map<String, Double> expectedMap = new LinkedHashMap<>();
    Map<String, Double> actualMap = new LinkedHashMap<>();

    for (GanttColumnSum item : expected) {
      expectedMap.put(item.getKey().toLowerCase(), item.getValue());
    }
    for (GanttColumnSum item : actual) {
      actualMap.put(item.getKey().toLowerCase(), item.getValue());
    }

    Assertions.assertEquals(expectedMap.keySet(), actualMap.keySet(), "Keys should match");

    for (String key : expectedMap.keySet()) {
      Double expectedValue = expectedMap.get(key);
      Double actualValue = actualMap.get(key);
      Assertions.assertEquals(expectedValue, actualValue, 0.001,
                              "Value mismatch for key: " + key);
    }
  }

  private String getGanttSumTestData(String fileName) throws IOException {
    return Files.readString(Paths.get("src", "test", "resources", "json", "gantt-sum", fileName));
  }

  private List<GanttColumnSum> getGanttSumActual(DStore dStore,
                                                 TProfile tProfile,
                                                 List<CProfile> cProfiles,
                                                 String firstColName,
                                                 String secondColName)
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    long first = dStore.getFirst(tProfile.getTableName(), Long.MIN_VALUE, Long.MAX_VALUE);
    long last = dStore.getLast(tProfile.getTableName(), Long.MIN_VALUE, Long.MAX_VALUE);
    return getGanttSumActual(dStore, tProfile, cProfiles, firstColName, secondColName, null, first, last);
  }

  private List<GanttColumnSum> getGanttSumActual(DStore dStore,
                                                 TProfile tProfile,
                                                 List<CProfile> cProfiles,
                                                 String firstColName,
                                                 String secondColName,
                                                 CompositeFilter compositeFilter)
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    long first = dStore.getFirst(tProfile.getTableName(), Long.MIN_VALUE, Long.MAX_VALUE);
    long last = dStore.getLast(tProfile.getTableName(), Long.MIN_VALUE, Long.MAX_VALUE);
    return getGanttSumActual(dStore, tProfile, cProfiles, firstColName, secondColName, compositeFilter, first, last);
  }

  private List<GanttColumnSum> getGanttSumActual(DStore dStore,
                                                 TProfile tProfile,
                                                 List<CProfile> cProfiles,
                                                 String firstColName,
                                                 String secondColName,
                                                 CompositeFilter compositeFilter,
                                                 long begin,
                                                 long end)
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    CProfile firstLevelGroupBy = findCProfile(cProfiles, firstColName);
    CProfile secondLevelGroupBy = findCProfile(cProfiles, secondColName);
    return dStore.getGanttSum(tProfile.getTableName(), firstLevelGroupBy, secondLevelGroupBy, compositeFilter, begin, end);
  }

  private CompositeFilter toCompositeFilter(List<CProfile> cProfiles, FilterSpec spec) {
    CProfile profile = findCProfile(cProfiles, spec.colName);
    FilterCondition condition = new FilterCondition(profile, spec.values, spec.compareFunction);
    return new CompositeFilter(List.of(condition), LogicalOperator.AND);
  }

  // ========== STACKED ==========
  private String runStackedTest(TestResources resources,
                                StackedTestDefinition testDef,
                                TProfile tProfile,
                                List<CProfile> cProfiles,
                                ObjectMapper objectMapper) {
    try {
      List<StackedColumn> expected = objectMapper.readValue(
          getStackedTestData(testDef.expectedJsonFileName),
          new TypeReference<>() {}
      );

      long begin;
      long end;
      if (testDef.useDateRange && testDef.date != null) {
        long[] range = toDayRangeMillis(testDef.date);
        begin = range[0];
        end = range[1];
      } else {
        begin = resources.dStore.getFirst(tProfile.getTableName(), Long.MIN_VALUE, Long.MAX_VALUE);
        end = resources.dStore.getLast(tProfile.getTableName(), Long.MIN_VALUE, Long.MAX_VALUE);
      }

      CProfile cProfile = findCProfile(cProfiles, testDef.colName);

      Instant start = Instant.now();
      List<StackedColumn> actual = resources.dStore.getStacked(
          tProfile.getTableName(), cProfile, GroupFunction.COUNT, null, begin, end);

      // Normalize actual data to a single aggregated series map
      List<StackedColumn> actualNormalized = normalizeStacked(actual);
      double seconds = Duration.between(start, Instant.now()).toMillis() / 1000.0;

      assertStackedListEquals(expected, actualNormalized);
      assertStackedMapEquals(expected, actualNormalized);

      return String.format("%.1f", seconds);
    } catch (Exception e) {
      log.error("STACKED test failed: {}", testDef.testName, e);
      return "ERROR";
    }
  }

  private String getStackedTestData(String fileName) throws IOException {
    return Files.readString(Paths.get("src", "test", "resources", "json", "stacked", fileName));
  }

  private List<StackedColumn> normalizeStacked(List<StackedColumn> stackedColumns) {
    // Merge all batches into one aggregated StackedColumn with summed counts per key
    Map<String, Integer> totals = stackedColumns.stream()
        .flatMap(sc -> sc.getKeyCount().entrySet().stream())
        .collect(java.util.stream.Collectors.groupingBy(
            Map.Entry::getKey, java.util.stream.Collectors.summingInt(Map.Entry::getValue)));

    StackedColumn merged = new StackedColumn();
    merged.setKey(0);
    merged.setTail(0);
    merged.getKeyCount().putAll(totals);

    return List.of(merged);
  }

  private void assertStackedMapEquals(List<StackedColumn> expected, List<StackedColumn> actual) {
    Map<String, Integer> expectedMap = expected.getFirst().getKeyCount();
    Map<String, Integer> actualMap = actual.getFirst().getKeyCount();
    Assertions.assertEquals(expectedMap, actualMap, "Stacked column maps do not match");
  }

  private void assertStackedListEquals(List<StackedColumn> expected, List<StackedColumn> actual) {
    Map<String, Integer> expectedMap = expected.getFirst().getKeyCount();
    Map<String, Integer> actualMap = actual.getFirst().getKeyCount();
    Assertions.assertEquals(expectedMap, actualMap, "Stacked column list contents do not match");
  }

  // ========== Helpers ==========
  private CProfile findCProfile(List<CProfile> cProfiles, String colName) {
    return cProfiles.stream()
        .filter(c -> c.getColName().equalsIgnoreCase(colName))
        .findFirst()
        .orElseThrow();
  }

  private long calculateFolderSize(Path path) {
    try (Stream<Path> paths = Files.walk(path)) {
      return paths
          .filter(p -> p.toFile().isFile())
          .mapToLong(p -> p.toFile().length())
          .sum();
    } catch (IOException e) {
      log.error("Failed to calculate directory size", e);
      return 0;
    }
  }

  private static void writeReportToFile() {
    String homeDir = System.getProperty("user.home");
    Path reportPath = Paths.get(homeDir, "dbase_ch_gantt_report.md");
    String combinedReport =
        "# PROFILE LOAD REPORT" + NL + NL +
            markdownTableByProfile + NL + NL +
            "## GANTT COUNT" + NL + markdownTableByGanttCount + NL + NL +
            "## GANTT SUM" + NL + markdownTableByGanttSum + NL + NL +
            "## STACKED" + NL + markdownTableByStacked + NL;

    try {
      Files.write(reportPath, combinedReport.getBytes());
      log.info("Markdown report saved to: {}", reportPath);
    } catch (IOException e) {
      log.error("Failed to write markdown report to file: {}", reportPath, e);
    }
  }

  @AfterAll
  public static void printMarkdownReport() {
    buildProfileTable();
    log.info(NL + NL + "MARKDOWN PROFILE REPORT:" + NL + markdownTableByProfile);

    buildMatrixTable("GANTT COUNT", markdownTableByGanttCount, testResultsCount);
    buildMatrixTable("GANTT SUM", markdownTableByGanttSum, testResultsSum);
    buildMatrixTable("STACKED", markdownTableByStacked, testResultsStacked);

    log.info(NL + "MARKDOWN GANTT COUNT REPORT:" + NL + markdownTableByGanttCount);
    log.info(NL + "MARKDOWN GANTT SUM REPORT:" + NL + markdownTableByGanttSum);
    log.info(NL + "MARKDOWN STACKED REPORT:" + NL + markdownTableByStacked);

    writeReportToFile();
  }

  private static void buildProfileTable() {
    if (profileResults.isEmpty()) {
      return;
    }

    markdownTableByProfile.append("| Property |");
    for (int i = 1; i <= profileResults.size(); i++) {
      markdownTableByProfile.append(" № ").append(i).append(" |");
    }
    markdownTableByProfile.append(NL);

    markdownTableByProfile.append("|---|");
    for (int i = 0; i < profileResults.size(); i++) {
      markdownTableByProfile.append("---|");
    }
    markdownTableByProfile.append(NL);

    // Get the keys from the first result map, assuming all maps have the same keys/order
    Map<String, String> firstResult = profileResults.getFirst();
    for (String key : firstResult.keySet()) {
      markdownTableByProfile.append("| ").append(key).append(" |");
      for (Map<String, String> resultMap : profileResults) {
        markdownTableByProfile.append(" ").append(resultMap.get(key)).append(" |");
      }
      markdownTableByProfile.append(NL);
    }
  }


  private static void buildMatrixTable(String title,
                                       StringBuilder tableBuilder,
                                       Map<String, List<String>> resultsByTest) {
    tableBuilder.append("| № | Test name |");
    for (int i = 1; i <= profileKeys.size(); i++) {
      tableBuilder.append(" № ").append(i).append(" |");
    }
    tableBuilder.append(NL).append("|--|--|");
    profileKeys.forEach(k -> tableBuilder.append("--|"));
    tableBuilder.append(NL);

    int testIndex = 1;
    for (Map.Entry<String, List<String>> entry : resultsByTest.entrySet()) {
      tableBuilder.append("| ").append(testIndex++).append(" | ")
          .append(entry.getKey()).append(" |");
      for (String result : entry.getValue()) {
        tableBuilder.append(" ").append(result).append(" |");
      }
      tableBuilder.append(NL);
    }
  }

  private record GanttCountTestDefinition(String testName,
                                          String firstCol,
                                          String secondCol,
                                          String expectedJsonFileName) {}

  private record GanttSumTestDefinition(String testName,
                                        String firstCol,
                                        String secondCol,
                                        String expectedJsonFileName,
                                        FilterSpec filter) {}

  private record StackedTestDefinition(String testName,
                                       String colName,
                                       boolean useDateRange,
                                       LocalDate date,
                                       String expectedJsonFileName) {}

  private record FilterSpec(String colName,
                            String[] values,
                            CompareFunction compareFunction) {}

  private static long[] toDayRangeMillis(LocalDate day) {
    LocalDateTime beginLDT = day.atStartOfDay();
    LocalDateTime endLDT = day.atTime(23, 59, 59, 999_999_999);
    long begin = Timestamp.valueOf(beginLDT).getTime();
    long end = Timestamp.valueOf(endLDT).getTime();
    return new long[]{begin, end};
  }
}