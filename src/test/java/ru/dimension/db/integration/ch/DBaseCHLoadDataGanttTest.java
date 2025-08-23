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
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
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
import ru.dimension.db.model.output.GanttColumnCount;
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
public class DBaseCHLoadDataGanttTest implements ClickHouse {

  private static final String url = "jdbc:clickhouse://localhost:8123/?user=admin&password=admin";

  private static final StringBuilder markdownTableByProfile = new StringBuilder();
  private static final StringBuilder markdownTableByGantt = new StringBuilder();
  private static final String NL = System.lineSeparator();
  private static final int BATCH_SIZE = 2;

  private static final List<String> profileKeys = new ArrayList<>();
  private static final Map<String, List<String>> testResults = new LinkedHashMap<>();
  private static final List<GanttTestDefinition> ganttTestDefinitions = Arrays.asList(
      new GanttTestDefinition("getGanttRawRaw", "PICKUP_CDELIGIBIL", "VENDOR_ID", "pickup_cdeligibil__vendor_id.json"),
      new GanttTestDefinition("getGanttEnumEnum", "DROPOFF_PUMA", "DROPOFF_BOROCODE", "dropoff_puma__dropoff_borocode.json"),
      new GanttTestDefinition("getGanttHistHist", "TRIP_TYPE", "PICKUP_BORONAME", "trip_type__pickup_boroname.json"),
      new GanttTestDefinition("getGanttHistRaw", "TRIP_TYPE", "VENDOR_ID", "trip_type__vendor_id.json"),
      new GanttTestDefinition("getGanttHistEnum", "TRIP_TYPE", "DROPOFF_BORONAME", "trip_type__dropoff_boroname.json"),
      new GanttTestDefinition("getGanttEnumRaw", "DROPOFF_BORONAME", "VENDOR_ID", "dropoff_boroname__vendor_id.json"),
      new GanttTestDefinition("getGanttEnumHist", "DROPOFF_BORONAME", "PICKUP_BORONAME", "dropoff_boroname__pickup_boroname.json"),
      new GanttTestDefinition("getGanttRawHist", "PICKUP_CDELIGIBIL", "PICKUP_BORONAME", "pickup_cdeligibil__pickup_boroname.json"),
      new GanttTestDefinition("getGanttRawEnum", "PICKUP_CDELIGIBIL", "CAB_TYPE", "pickup_cdeligibil__cab_type.json")
  );

  static {
    // Initialize test results map
    for (GanttTestDefinition testDef : ganttTestDefinitions) {
      testResults.put(testDef.testName, new ArrayList<>());
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
      long loadSeconds = loadDuration.minusMinutes(loadMinutes).getSeconds();
      String loadTimeFormatted = String.format("%d min %d sec", loadMinutes, loadSeconds);

      long sizeBytes = calculateFolderSize(resources.databaseDir);
      double sizeGB = sizeBytes / (1024.0 * 1024.0 * 1024.0);

      appendMarkdownRow(tType, iType, aType, compression,
                        loadTimeFormatted,
                        String.format("%.3f", sizeGB));

      // Run all gantt tests by profileKey
      String profileKey = String.format("%s_%s_%s_%s", tType, iType, aType, compression);
      profileKeys.add(profileKey);

      runAllGanttTests(resources, profileKey);
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

  private void runAllGanttTests(TestResources resources, String profileKey) {
    try {
      // Get table profile
      TProfile tProfile = resources.dStore.getTProfile(ClickHouse.tableName);
      List<CProfile> cProfiles = tProfile.getCProfiles();
      ObjectMapper objectMapper = new ObjectMapper();

      for (GanttTestDefinition testDef : ganttTestDefinitions) {
        String result = runGanttTest(
            resources,
            testDef,
            tProfile,
            cProfiles,
            objectMapper
        );
        testResults.get(testDef.testName).add(result);
      }
    } catch (Exception e) {
      log.error("Failed to run gantt tests for profile {}", profileKey, e);
    }
  }

  private String runGanttTest(TestResources resources,
                              GanttTestDefinition testDef,
                              TProfile tProfile,
                              List<CProfile> cProfiles,
                              ObjectMapper objectMapper) {
    try {
      // Load expected results
      List<GanttColumnCount> expected = objectMapper.readValue(
          getGanttTestData(testDef.expectedJsonFileName),
          new TypeReference<>() {}
      );

      Instant singleStart = Instant.now();
      List<GanttColumnCount> actualSingle = getGanttDataActual(
          resources.dStore, tProfile, cProfiles,
          testDef.firstCol, testDef.secondCol
      );
      double singleTime = Duration.between(singleStart, Instant.now()).toMillis() / 1000.0;

      // Parallel execution
      Instant parallelStart = Instant.now();
      List<GanttColumnCount> actualParallel = getGanttDataActual(
          resources.dStore, tProfile, cProfiles,
          testDef.firstCol, testDef.secondCol, BATCH_SIZE
      );
      double parallelTime = Duration.between(parallelStart, Instant.now()).toMillis() / 1000.0;

      // Validate results
      assertGanttListEquals(expected, actualSingle);
      assertGanttMapEquals(expected, actualSingle);
      assertGanttListEquals(expected, actualParallel);
      assertGanttMapEquals(expected, actualParallel);

      return String.format("%.1f / %.1f", singleTime, parallelTime);
    } catch (Exception e) {
      log.error("Gantt test failed: {}", testDef.testName, e);
      return "ERROR";
    }
  }

  private void assertGanttMapEquals(List<GanttColumnCount> expected, List<GanttColumnCount> actual) {
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

  private void assertGanttListEquals(List<GanttColumnCount> expected, List<GanttColumnCount> actual) {
    assertTrue(
        expected.size() == actual.size() &&
            expected.containsAll(actual) &&
            actual.containsAll(expected)
    );
  }

  private String getGanttTestData(String fileName) throws IOException {
    return Files.readString(Paths.get("src", "test", "resources", "json", "gantt", fileName));
  }

  private List<GanttColumnCount> getGanttDataActual(
      DStore dStore, TProfile tProfile, List<CProfile> cProfiles,
      String firstColName, String secondColName
  ) throws Exception {
    long first = dStore.getFirst(tProfile.getTableName(), Long.MIN_VALUE, Long.MAX_VALUE);
    long last = dStore.getLast(tProfile.getTableName(), Long.MIN_VALUE, Long.MAX_VALUE);
    return getGanttDataActual(dStore, tProfile, cProfiles, firstColName, secondColName, first, last);
  }

  private List<GanttColumnCount> getGanttDataActual(
      DStore dStore, TProfile tProfile, List<CProfile> cProfiles,
      String firstColName, String secondColName, int batchSize
  ) throws Exception {
    long first = dStore.getFirst(tProfile.getTableName(), Long.MIN_VALUE, Long.MAX_VALUE);
    long last = dStore.getLast(tProfile.getTableName(), Long.MIN_VALUE, Long.MAX_VALUE);
    return getGanttDataActual(dStore, tProfile, cProfiles, firstColName, secondColName, batchSize, first, last);
  }

  private List<GanttColumnCount> getGanttDataActual(
      DStore dStore, TProfile tProfile, List<CProfile> cProfiles,
      String firstColName, String secondColName, long begin, long end
  ) throws Exception {
    CProfile firstLevel = findCProfile(cProfiles, firstColName);
    CProfile secondLevel = findCProfile(cProfiles, secondColName);
    return dStore.getGanttCount(tProfile.getTableName(), firstLevel, secondLevel, null, begin, end);
  }

  private List<GanttColumnCount> getGanttDataActual(
      DStore dStore, TProfile tProfile, List<CProfile> cProfiles,
      String firstColName, String secondColName, int batchSize, long begin, long end
  ) throws Exception {
    CProfile firstLevel = findCProfile(cProfiles, firstColName);
    CProfile secondLevel = findCProfile(cProfiles, secondColName);
    return dStore.getGanttCount(tProfile.getTableName(), firstLevel, secondLevel, null, batchSize, begin, end);
  }

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

  // Helper classes
  private static class GanttTestDefinition {
    final String testName;
    final String firstCol;
    final String secondCol;
    final String expectedJsonFileName;

    GanttTestDefinition(String testName, String firstCol, String secondCol, String expectedJsonFileName) {
      this.testName = testName;
      this.firstCol = firstCol;
      this.secondCol = secondCol;
      this.expectedJsonFileName = expectedJsonFileName;
    }
  }

  private static synchronized void appendMarkdownRow(TType tType,
                                                     IType iType,
                                                     AType aType,
                                                     boolean compression,
                                                     String loadTimeMin,
                                                     String sizeGB) {

    if (markdownTableByProfile.length() == 0) {
      markdownTableByProfile.append("| # | TType | IType | AType | Compression | Load (min) | Size (GB) |")
          .append(NL)
          .append("|--:|-------|-------|-------|-------------|------------|-----------|")
          .append(NL);
    }

    markdownTableByProfile.append("| ")
        .append(getRowCounter()).append(" | ")
        .append(tType).append(" | ")
        .append(iType).append(" | ")
        .append(aType).append(" | ")
        .append(compression).append(" | ")
        .append(loadTimeMin).append(" | ")
        .append(sizeGB).append(" |")
        .append(NL);
  }

  private static int rowCounter = 1;
  private static synchronized int getRowCounter() {
    return rowCounter++;
  }

  private static void writeReportToFile() {
    String homeDir = System.getProperty("user.home");
    Path reportPath = Paths.get(homeDir, "dbase_ch_gantt_report.md");
    String combinedReport = markdownTableByProfile + NL + NL + markdownTableByGantt;

    try {
      Files.write(reportPath, combinedReport.getBytes());
      log.info("Markdown report saved to: {}", reportPath);
    } catch (IOException e) {
      log.error("Failed to write markdown report to file: {}", reportPath, e);
    }
  }

  @AfterAll
  public static void printMarkdownReport() {
    log.info(NL + NL + "MARKDOWN PROFILE REPORT:" + NL + markdownTableByProfile);

    markdownTableByGantt.append("| № | Test name |");
    for (String profileKey : profileKeys) {
      String[] parts = profileKey.split("_");
      String shortName = parts[parts.length - 3] + "_" + parts[parts.length - 2];
      markdownTableByGantt.append(" ").append(shortName).append(" <br/>Execution time <br/>(single/2-thread) (sec) |");
    }
    markdownTableByGantt.append(NL).append("|--|--|");
    profileKeys.forEach(k -> markdownTableByGantt.append("--|"));
    markdownTableByGantt.append(NL);

    int testIndex = 1;
    for (GanttTestDefinition testDef : ganttTestDefinitions) {
      markdownTableByGantt.append("| ").append(testIndex++).append(" | ")
          .append(testDef.testName).append(" |");

      for (String result : testResults.get(testDef.testName)) {
        markdownTableByGantt.append(" ").append(result).append(" |");
      }
      markdownTableByGantt.append(NL);
    }

    log.info(NL + "MARKDOWN GANTT REPORT:" + NL + markdownTableByGantt);

    writeReportToFile();
  }
}