package ru.dimension.db.integration.ch;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.IntSummaryStatistics;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.log4j.Log4j2;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import ru.dimension.db.DBase;
import ru.dimension.db.backend.BerkleyDB;
import ru.dimension.db.config.DBaseConfig;
import ru.dimension.db.core.DStore;
import ru.dimension.db.exception.BeginEndWrongOrderException;
import ru.dimension.db.exception.SqlColMetadataException;
import ru.dimension.db.exception.TableNameEmptyException;
import ru.dimension.db.model.GroupFunction;
import ru.dimension.db.model.output.StackedColumn;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.TProfile;
import ru.dimension.db.source.ClickHouse;

@Log4j2
@TestInstance(Lifecycle.PER_CLASS)
@Disabled
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class DBaseCHQueryStackedTest implements ClickHouse {

  private static final String TEST_DATA_BASE_PATH = "src/test/resources/json/stacked";
  private static final StringBuilder markdownTable = new StringBuilder();
  private static final StringBuilder markdownQueryTable = new StringBuilder();
  private int testCounter = 1;

  static {
    // Main results table for single-threaded execution
    markdownTable.append("| № | Test name | Execution time (ms) |\n");
    markdownTable.append("|---|---|---|\n");

    // Query table
    markdownQueryTable.append("| № | Test name | SQL query |\n");
    markdownQueryTable.append("|---|---|---|\n");
  }

  private DStore dStore;
  private TProfile tProfile;
  private BerkleyDB berkleyDB;
  private ObjectMapper objectMapper;

  @BeforeAll
  public void initialLoading() throws IOException {
    String dbFolder = getTestDbFolder();
    this.berkleyDB = new BerkleyDB(dbFolder, false);

    DBaseConfig dBaseConfig = new DBaseConfig().setConfigDirectory(dbFolder);
    DBase dBase = new DBase(dBaseConfig, berkleyDB.getStore());
    dStore = dBase.getDStore();

    tProfile = getTProfileSafe(dBase);
    objectMapper = new ObjectMapper();
  }

  @Test
  @Order(1)
  public void stackedHist() throws BeginEndWrongOrderException, SqlColMetadataException, IOException {
    String testName = "stackedHist";
    String query = """
            SELECT trip_type, COUNT(trip_type)
            FROM datasets.trips_mergetree
            WHERE toYear(pickup_date) = 2016
            GROUP BY trip_type;
            """;
    log.info("Query:\n{}", query);

    long executionTimeMs;
    List<StackedColumn> actual;

    CProfile cProfile = getCProfileByName("TRIP_TYPE");
    long first = dStore.getFirst(tProfile.getTableName(), Long.MIN_VALUE, Long.MAX_VALUE);
    long last = dStore.getLast(tProfile.getTableName(), Long.MIN_VALUE, Long.MAX_VALUE);

    List<StackedColumn> expected = getStackedDataExpected("trip_type.json");

    // Single-threaded execution
    Instant start = Instant.now();
    try {
      actual = dStore.getStacked(tProfile.getTableName(), cProfile, GroupFunction.COUNT, null, first, last);
      actual = processActualResults(actual);
    } finally {
      Instant end = Instant.now();
      executionTimeMs = Duration.between(start, end).toMillis();
    }

    log.info("Expected: {}", expected);
    log.info("Actual: {}", actual);

    assertStackedResults(expected, actual);

    // Update markdown tables
    updateMarkdownTables(testName, query, executionTimeMs);
  }

  @Test
  @Order(2)
  public void stackedHistDate() throws BeginEndWrongOrderException, SqlColMetadataException, IOException {
    String testName = "stackedHistDate";
    String query = """
            SELECT trip_type, COUNT(trip_type)
            FROM datasets.trips_mergetree
            WHERE toYYYYMMDD(pickup_datetime) = 20160101
            GROUP BY trip_type;
            """;
    log.info("Query:\n{}", query);

    long executionTimeMs;
    List<StackedColumn> actual;

    CProfile cProfile = getCProfileByName("TRIP_TYPE");
    TimeRange timeRange = createTimeRange(2016, 1, 1);

    List<StackedColumn> expected = getStackedDataExpected("trip_type_date.json");

    // Single-threaded execution
    Instant start = Instant.now();
    try {
      actual = dStore.getStacked(tProfile.getTableName(), cProfile, GroupFunction.COUNT, null, timeRange.begin(), timeRange.end());
      actual = mergeStackedColumns(actual);
    } finally {
      Instant end = Instant.now();
      executionTimeMs = Duration.between(start, end).toMillis();
    }

    log.info("Expected: {}", expected);
    log.info("Actual: {}", actual);

    assertStackedResults(expected, actual);

    // Update markdown tables
    updateMarkdownTables(testName, query, executionTimeMs);
  }

  @Test
  @Order(3)
  public void stackedEnum() throws BeginEndWrongOrderException, SqlColMetadataException, IOException {
    String testName = "stackedEnum";
    String query = """
          SELECT dropoff_boroname, COUNT(dropoff_boroname)
          FROM datasets.trips_mergetree
          WHERE toYear(pickup_date) = 2016
          GROUP BY dropoff_boroname;
          """;
    log.info("Query:\n{}", query);

    long executionTimeMs;
    List<StackedColumn> actual;

    CProfile cProfile = getCProfileByName("DROPOFF_BORONAME");
    long first = dStore.getFirst(tProfile.getTableName(), Long.MIN_VALUE, Long.MAX_VALUE);
    long last = dStore.getLast(tProfile.getTableName(), Long.MIN_VALUE, Long.MAX_VALUE);

    List<StackedColumn> expected = getStackedDataExpected("dropoff_boroname.json");

    // Single-threaded execution
    Instant start = Instant.now();
    try {
      actual = dStore.getStacked(tProfile.getTableName(), cProfile, GroupFunction.COUNT, null, first, last);
      actual = processActualResults(actual);
    } finally {
      Instant end = Instant.now();
      executionTimeMs = Duration.between(start, end).toMillis();
    }

    log.info("Expected: {}", expected);
    log.info("Actual: {}", actual);

    assertStackedResults(expected, actual);

    // Update markdown tables
    updateMarkdownTables(testName, query, executionTimeMs);
  }

  @Test
  @Order(4)
  public void stackedEnumDate() throws BeginEndWrongOrderException, SqlColMetadataException, IOException {
    String testName = "stackedEnumDate";
    String query = """
            SELECT dropoff_boroname, COUNT(dropoff_boroname)
            FROM datasets.trips_mergetree
            WHERE toYYYYMMDD(pickup_datetime) = 20160101
            GROUP BY dropoff_boroname;
            """;
    log.info("Query:\n{}", query);

    long executionTimeMs;
    List<StackedColumn> actual;

    CProfile cProfile = getCProfileByName("DROPOFF_BORONAME");
    TimeRange timeRange = createTimeRange(2016, 1, 1);

    List<StackedColumn> expected = getStackedDataExpected("dropoff_boroname_date.json");

    // Single-threaded execution
    Instant start = Instant.now();
    try {
      actual = dStore.getStacked(tProfile.getTableName(), cProfile, GroupFunction.COUNT, null, timeRange.begin(), timeRange.end());
      actual = mergeStackedColumns(actual);
    } finally {
      Instant end = Instant.now();
      executionTimeMs = Duration.between(start, end).toMillis();
    }

    log.info("Expected: {}", expected);
    log.info("Actual: {}", actual);

    assertStackedResults(expected, actual);

    // Update markdown tables
    updateMarkdownTables(testName, query, executionTimeMs);
  }

  @Test
  @Order(5)
  public void stackedRaw() throws BeginEndWrongOrderException, SqlColMetadataException, IOException {
    String testName = "stackedRaw";
    String query = """
          SELECT vendor_id, COUNT(vendor_id)
          FROM datasets.trips_mergetree
          WHERE toYear(pickup_date) = 2016
          GROUP BY vendor_id;
          """;
    log.info("Query:\n{}", query);

    long executionTimeMs;
    List<StackedColumn> actual;

    CProfile cProfile = getCProfileByName("VENDOR_ID");
    long first = dStore.getFirst(tProfile.getTableName(), Long.MIN_VALUE, Long.MAX_VALUE);
    long last = dStore.getLast(tProfile.getTableName(), Long.MIN_VALUE, Long.MAX_VALUE);

    List<StackedColumn> expected = getStackedDataExpected("vendor_id.json");

    // Single-threaded execution
    Instant start = Instant.now();
    try {
      actual = dStore.getStacked(tProfile.getTableName(), cProfile, GroupFunction.COUNT, null, first, last);
      actual = processActualResults(actual);
    } finally {
      Instant end = Instant.now();
      executionTimeMs = Duration.between(start, end).toMillis();
    }

    log.info("Expected: {}", expected);
    log.info("Actual: {}", actual);

    assertStackedResults(expected, actual);

    // Update markdown tables
    updateMarkdownTables(testName, query, executionTimeMs);
  }

  @Test
  @Order(6)
  public void stackedRawDate() throws BeginEndWrongOrderException, SqlColMetadataException, IOException {
    String testName = "stackedRawDate";
    String query = """
            SELECT vendor_id, COUNT(vendor_id)
            FROM datasets.trips_mergetree
            WHERE toYYYYMMDD(pickup_datetime) = 20160101
            GROUP BY vendor_id;
            """;
    log.info("Query:\n{}", query);

    long executionTimeMs;
    List<StackedColumn> actual;

    CProfile cProfile = getCProfileByName("VENDOR_ID");
    TimeRange timeRange = createTimeRange(2016, 1, 1);

    List<StackedColumn> expected = getStackedDataExpected("vendor_id_date.json");

    // Single-threaded execution
    Instant start = Instant.now();
    try {
      actual = dStore.getStacked(tProfile.getTableName(), cProfile, GroupFunction.COUNT, null, timeRange.begin(), timeRange.end());
      actual = mergeStackedColumns(actual);
    } finally {
      Instant end = Instant.now();
      executionTimeMs = Duration.between(start, end).toMillis();
    }

    log.info("Expected: {}", expected);
    log.info("Actual: {}", actual);

    assertStackedResults(expected, actual);

    // Update markdown tables
    updateMarkdownTables(testName, query, executionTimeMs);
  }

  @AfterAll
  public void closeDb() {
    berkleyDB.closeDatabase();

    log.info("\nResults Table:\n" + markdownTable);
    log.info("\nQueries Table:\n" + markdownQueryTable);
  }

  private void updateMarkdownTables(String testName, String query, long executionTimeMs) {
    String formattedTime = formatExecutionTime(executionTimeMs);

    markdownTable.append("| ").append(testCounter).append(" | ")
        .append(testName).append(" | ")
        .append(formattedTime)
        .append(" |\n");

    markdownQueryTable.append("| ").append(testCounter).append(" | ")
        .append(testName).append(" | ")
        .append("`").append(formatQueryForMarkdown(query)).append("`").append(" |\n");

    testCounter++;
  }

  private String formatExecutionTime(long executionTimeMs) {
    long minutes = executionTimeMs / 60000;
    long seconds = (executionTimeMs % 60000) / 1000;
    long milliseconds = executionTimeMs % 1000;

    if (minutes > 0) {
      return String.format("%d minute%s %d second%s",
                           minutes, minutes != 1 ? "s" : "",
                           seconds, seconds != 1 ? "s" : "");
    } else if (seconds > 0) {
      if (milliseconds > 0) {
        return String.format("%d second%s %d ms",
                             seconds, seconds != 1 ? "s" : "", milliseconds);
      } else {
        return String.format("%d second%s",
                             seconds, seconds != 1 ? "s" : "");
      }
    } else {
      return String.format("%d ms", milliseconds);
    }
  }

  private String formatQueryForMarkdown(String query) {
    return query.replace("\n", " ").replaceAll("\\s+", " ").trim();
  }

  private TProfile getTProfileSafe(DBase dBase) {
    try {
      TProfile profile = dBase.getDStore().getTProfile(tableName);
      log.info("Loaded table profile: {}", profile);
      return profile;
    } catch (TableNameEmptyException e) {
      throw new RuntimeException("Failed to load table profile", e);
    }
  }

  private CProfile getCProfileByName(String colName) {
    return tProfile.getCProfiles().stream()
        .filter(profile -> profile.getColName().equals(colName))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Column profile not found: " + colName));
  }

  private List<StackedColumn> processActualResults(List<StackedColumn> actual) {
    StackedColumn resultColumn = createStackedColumn(0, 0);
    List<StackedColumn> actualResult = List.of(resultColumn);

    Map<String, IntSummaryStatistics> batchData = aggregateBatchData(actual);
    Set<String> series = collectSeries(actual);

    series.forEach(seriesName -> {
      int sum = getSeriesSum(batchData, seriesName);
      resultColumn.getKeyCount().put(seriesName, sum);
    });

    return actualResult;
  }

  private Set<String> collectSeries(List<StackedColumn> stackedColumns) {
    return stackedColumns.stream()
        .map(StackedColumn::getKeyCount)
        .map(Map::keySet)
        .flatMap(Collection::stream)
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  private Map<String, IntSummaryStatistics> aggregateBatchData(List<StackedColumn> stackedColumns) {
    return stackedColumns.stream()
        .map(StackedColumn::getKeyCount)
        .flatMap(map -> map.entrySet().stream())
        .collect(Collectors.groupingBy(
            Map.Entry::getKey,
            Collectors.summarizingInt(Map.Entry::getValue)
        ));
  }

  private int getSeriesSum(Map<String, IntSummaryStatistics> batchData, String seriesName) {
    return Math.toIntExact(Optional.ofNullable(batchData.get(seriesName))
                               .map(IntSummaryStatistics::getSum)
                               .orElse(0L));
  }

  private TimeRange createTimeRange(int year, int month, int day) {
    LocalDateTime beginLDT = LocalDateTime.of(year, month, day, 0, 0, 0, 0);
    LocalDateTime endLDT = LocalDateTime.of(year, month, day, 23, 59, 59, 999999999);

    long begin = Timestamp.valueOf(beginLDT).getTime();
    long end = Timestamp.valueOf(endLDT).getTime();

    return new TimeRange(begin, end);
  }

  private List<StackedColumn> mergeStackedColumns(List<StackedColumn> stackedColumns) {
    Map<String, Integer> totals = stackedColumns.stream()
        .flatMap(column -> column.getKeyCount().entrySet().stream())
        .collect(Collectors.groupingBy(
            Map.Entry::getKey,
            Collectors.summingInt(Map.Entry::getValue)
        ));

    StackedColumn merged = createStackedColumn(0, 0);
    merged.getKeyCount().putAll(totals);

    return List.of(merged);
  }

  private StackedColumn createStackedColumn(int key, int tail) {
    StackedColumn column = new StackedColumn();
    column.setKey(key);
    column.setTail(tail);
    return column;
  }

  private void assertStackedResults(List<StackedColumn> expected, List<StackedColumn> actual) {
    assertStackedListEquals(expected, actual);
    assertStackedMapEquals(expected, actual);
  }

  private void assertStackedMapEquals(List<StackedColumn> expected, List<StackedColumn> actual) {
    Map<String, Integer> expectedMap = expected.getFirst().getKeyCount();
    Map<String, Integer> actualMap = actual.getFirst().getKeyCount();
    assertEquals(expectedMap, actualMap, "Stacked column maps do not match");
  }

  private void assertStackedListEquals(List<StackedColumn> expected, List<StackedColumn> actual) {
    Map<String, Integer> expectedMap = expected.getFirst().getKeyCount();
    Map<String, Integer> actualMap = actual.getFirst().getKeyCount();
    assertEquals(expectedMap, actualMap, "Stacked column list contents do not match");
  }

  private void assertData(String expectedJsonFile, List<StackedColumn> actual) throws IOException {
    List<StackedColumn> expected = getStackedDataExpected(expectedJsonFile);
    log.info("Expected: {}", expected);
    log.info("Actual: {}", actual);
    assertStackedListEquals(expected, actual);
  }

  protected List<StackedColumn> getStackedDataExpected(String fileName) throws IOException {
    String jsonData = getStackedTestData(fileName);
    return objectMapper.readValue(jsonData, new TypeReference<>() {});
  }

  private String getStackedTestData(String fileName) throws IOException {
    return Files.readString(Paths.get(TEST_DATA_BASE_PATH, fileName));
  }

  private record TimeRange(long begin, long end) {}
}