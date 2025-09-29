package ru.dimension.db.integration.ch;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.log4j.Log4j2;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import ru.dimension.db.DBase;
import ru.dimension.db.backend.BerkleyDB;
import ru.dimension.db.config.DBaseConfig;
import ru.dimension.db.core.DStore;
import ru.dimension.db.exception.BeginEndWrongOrderException;
import ru.dimension.db.exception.GanttColumnNotSupportedException;
import ru.dimension.db.exception.SqlColMetadataException;
import ru.dimension.db.exception.TableNameEmptyException;
import ru.dimension.db.model.CompareFunction;
import ru.dimension.db.model.filter.CompositeFilter;
import ru.dimension.db.model.filter.FilterCondition;
import ru.dimension.db.model.filter.LogicalOperator;
import ru.dimension.db.model.output.GanttColumnSum;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.TProfile;
import ru.dimension.db.source.ClickHouse;

@Log4j2
@TestInstance(Lifecycle.PER_CLASS)
@Disabled
public class DBaseCHQueryGanttSumTest implements ClickHouse {
  private static final StringBuilder markdownTable = new StringBuilder();
  private static final StringBuilder markdownQueryTable = new StringBuilder();
  private int testCounter = 1;

  static {
    // Main results table
    markdownTable.append("| № | Test name | Execution time (sec) |\n");
    markdownTable.append("|---|---|---|\n");

    // Query table
    markdownQueryTable.append("| № | Test name | SQL query |\n");
    markdownQueryTable.append("|---|---|---|\n");
  }

  private DStore dStore;
  private TProfile tProfile;
  private List<CProfile> cProfiles;

  private BerkleyDB berkleyDB;
  private ObjectMapper objectMapper;

  @BeforeAll
  public void initialLoading() throws IOException {
    String dbFolder = getTestDbFolder();

    this.berkleyDB = new BerkleyDB(dbFolder, false);

    DBaseConfig dBaseConfig = new DBaseConfig().setConfigDirectory(dbFolder);
    DBase dBase = new DBase(dBaseConfig, berkleyDB.getStore());
    dStore = dBase.getDStore();

    try {
      tProfile = dBase.getDStore().getTProfile(tableName);
      log.info(tProfile);
    } catch (TableNameEmptyException e) {
      throw new RuntimeException(e);
    }
    cProfiles = tProfile.getCProfiles();

    objectMapper = new ObjectMapper();
  }

  private String formatQueryForMarkdown(String query) {
    return query.replace("\n", " ").replaceAll("\\s+", " ").trim();
  }

  @Test
  public void getGanttSumHistRaw()
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException, IOException {
    String testName = "getGanttSumHistRaw";
    String query = """
                 SELECT trip_type, SUM(fare_amount)
                 FROM datasets.trips_mergetree
                 WHERE toYear(pickup_date) = 2016
                 GROUP BY trip_type;
            """;
    log.info("Query: " + "\n" + query);

    List<GanttColumnSum> expected = getGanttSumDataExpected("trip_type__fare_amount.json");

    double executionTime;
    List<GanttColumnSum> actual;

    Instant start = Instant.now();
    try {
      actual = getGanttSumDataActual("TRIP_TYPE", "FARE_AMOUNT");
    } finally {
      Instant end = Instant.now();
      executionTime = Duration.between(start, end).toMillis() / 1000.0;
    }

    log.info("Expected: " + expected);
    log.info("Actual: " + actual);

    assertGanttSumListEquals(expected, actual);

    markdownTable.append("| ").append(testCounter).append(" | ")
        .append(testName).append(" | ")
        .append(String.format("%.1f", executionTime))
        .append(" |\n");
    testCounter++;

    markdownQueryTable.append("| ").append(testCounter-1).append(" | ")
        .append(testName).append(" | ")
        .append("`").append(formatQueryForMarkdown(query)).append("`").append(" |\n");
  }

  @Test
  public void getGanttSumEnumRaw()
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException, IOException {
    String testName = "getGanttSumEnumRaw";
    String query = """
                 SELECT pickup_boroname, SUM(trip_distance)
                 FROM datasets.trips_mergetree
                 WHERE toYear(pickup_date) = 2016
                 GROUP BY pickup_boroname;
            """;
    log.info("Query: " + "\n" + query);

    List<GanttColumnSum> expected = getGanttSumDataExpected("pickup_boroname__trip_distance.json");

    double executionTime;
    List<GanttColumnSum> actual;

    Instant start = Instant.now();
    try {
      actual = getGanttSumDataActual("PICKUP_BORONAME", "TRIP_DISTANCE");
    } finally {
      Instant end = Instant.now();
      executionTime = Duration.between(start, end).toMillis() / 1000.0;
    }

    log.info("Expected: " + expected);
    log.info("Actual: " + actual);

    assertGanttSumListEquals(expected, actual);

    markdownTable.append("| ").append(testCounter).append(" | ")
        .append(testName).append(" | ")
        .append(String.format("%.1f", executionTime))
        .append(" |\n");
    testCounter++;

    markdownQueryTable.append("| ").append(testCounter-1).append(" | ")
        .append(testName).append(" | ")
        .append("`").append(formatQueryForMarkdown(query)).append("`").append(" |\n");
  }

  @Test
  public void getGanttSumRawRaw()
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException, IOException {
    String testName = "getGanttSumRawRaw";
    String query = """
                 SELECT vendor_id, SUM(total_amount)
                 FROM datasets.trips_mergetree
                 WHERE toYear(pickup_date) = 2016
                 GROUP BY vendor_id;
            """;
    log.info("Query: " + "\n" + query);

    List<GanttColumnSum> expected = getGanttSumDataExpected("vendor_id__total_amount.json");

    double executionTime;
    List<GanttColumnSum> actual;

    Instant start = Instant.now();
    try {
      actual = getGanttSumDataActual("VENDOR_ID", "TOTAL_AMOUNT");
    } finally {
      Instant end = Instant.now();
      executionTime = Duration.between(start, end).toMillis() / 1000.0;
    }

    log.info("Expected: " + expected);
    log.info("Actual: " + actual);

    assertGanttSumListEquals(expected, actual);

    markdownTable.append("| ").append(testCounter).append(" | ")
        .append(testName).append(" | ")
        .append(String.format("%.1f", executionTime))
        .append(" |\n");
    testCounter++;

    markdownQueryTable.append("| ").append(testCounter-1).append(" | ")
        .append(testName).append(" | ")
        .append("`").append(formatQueryForMarkdown(query)).append("`").append(" |\n");
  }

  @Test
  public void getGanttSumHistHist()
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException, IOException {
    String testName = "getGanttSumHistHist";
    String query = """
                 SELECT trip_type, SUM(passenger_count)
                 FROM datasets.trips_mergetree
                 WHERE toYear(pickup_date) = 2016
                 GROUP BY trip_type;
            """;
    log.info("Query: " + "\n" + query);

    List<GanttColumnSum> expected = getGanttSumDataExpected("trip_type__passenger_count.json");

    double executionTime;
    List<GanttColumnSum> actual;

    Instant start = Instant.now();
    try {
      actual = getGanttSumDataActual("TRIP_TYPE", "PASSENGER_COUNT");
    } finally {
      Instant end = Instant.now();
      executionTime = Duration.between(start, end).toMillis() / 1000.0;
    }

    log.info("Expected: " + expected);
    log.info("Actual: " + actual);

    assertGanttSumListEquals(expected, actual);

    markdownTable.append("| ").append(testCounter).append(" | ")
        .append(testName).append(" | ")
        .append(String.format("%.1f", executionTime))
        .append(" |\n");
    testCounter++;

    markdownQueryTable.append("| ").append(testCounter-1).append(" | ")
        .append(testName).append(" | ")
        .append("`").append(formatQueryForMarkdown(query)).append("`").append(" |\n");
  }

  @Test
  public void getGanttSumEnumEnum()
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException, IOException {
    String testName = "getGanttSumEnumEnum";
    String query = """
                 SELECT dropoff_boroname, SUM(dropoff_borocode)
                 FROM datasets.trips_mergetree
                 WHERE toYear(pickup_date) = 2016
                 GROUP BY dropoff_boroname;
            """;
    log.info("Query: " + "\n" + query);

    List<GanttColumnSum> expected = getGanttSumDataExpected("dropoff_boroname__dropoff_borocode.json");

    double executionTime;
    List<GanttColumnSum> actual;

    Instant start = Instant.now();
    try {
      actual = getGanttSumDataActual("DROPOFF_BORONAME", "DROPOFF_BOROCODE");
    } finally {
      Instant end = Instant.now();
      executionTime = Duration.between(start, end).toMillis() / 1000.0;
    }

    log.info("Expected: " + expected);
    log.info("Actual: " + actual);

    assertGanttSumListEquals(expected, actual);

    markdownTable.append("| ").append(testCounter).append(" | ")
        .append(testName).append(" | ")
        .append(String.format("%.1f", executionTime))
        .append(" |\n");
    testCounter++;

    markdownQueryTable.append("| ").append(testCounter-1).append(" | ")
        .append(testName).append(" | ")
        .append("`").append(formatQueryForMarkdown(query)).append("`").append(" |\n");
  }

  @Test
  public void getGanttSumRawEnum()
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException, IOException {
    String testName = "getGanttSumRawEnum";
    String query = """
                 SELECT pickup_cdeligibil, SUM(pickup_borocode)
                 FROM datasets.trips_mergetree
                 WHERE toYear(pickup_date) = 2016
                 GROUP BY pickup_cdeligibil;
            """;
    log.info("Query: " + "\n" + query);

    List<GanttColumnSum> expected = getGanttSumDataExpected("pickup_cdeligibil__pickup_borocode.json");

    double executionTime;
    List<GanttColumnSum> actual;

    Instant start = Instant.now();
    try {
      actual = getGanttSumDataActual("PICKUP_CDELIGIBIL", "PICKUP_BOROCODE");
    } finally {
      Instant end = Instant.now();
      executionTime = Duration.between(start, end).toMillis() / 1000.0;
    }

    log.info("Expected: " + expected);
    log.info("Actual: " + actual);

    assertGanttSumListEquals(expected, actual);

    markdownTable.append("| ").append(testCounter).append(" | ")
        .append(testName).append(" | ")
        .append(String.format("%.1f", executionTime))
        .append(" |\n");
    testCounter++;

    markdownQueryTable.append("| ").append(testCounter-1).append(" | ")
        .append(testName).append(" | ")
        .append("`").append(formatQueryForMarkdown(query)).append("`").append(" |\n");
  }

  @Test
  public void getGanttSumHistRawWithFilter()
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException, IOException {
    String testName = "getGanttSumWithFilter";
    String query = """
               SELECT trip_type, SUM(fare_amount)
               FROM datasets.trips_mergetree
               WHERE toYear(pickup_date) = 2016 AND trip_type IN (1, 2)
               GROUP BY trip_type;
          """;
    log.info("Query: " + "\n" + query);

    List<GanttColumnSum> expected = getGanttSumDataExpected("trip_type__fare_amount_sum_filtered.json");

    double executionTime;
    List<GanttColumnSum> actual;

    CProfile tripTypeProfile = cProfiles.stream()
        .filter(c -> c.getColName().equalsIgnoreCase("TRIP_TYPE"))
        .findFirst()
        .orElseThrow();

    FilterCondition condition = new FilterCondition(
        tripTypeProfile,
        new String[]{"1", "2"},
        CompareFunction.EQUAL
    );

    CompositeFilter compositeFilter = new CompositeFilter(
        List.of(condition),
        LogicalOperator.AND
    );

    Instant start = Instant.now();
    try {
      actual = getGanttSumDataActual("TRIP_TYPE", "FARE_AMOUNT", compositeFilter);
    } finally {
      Instant end = Instant.now();
      executionTime = Duration.between(start, end).toMillis() / 1000.0;
    }

    log.info("Expected: " + expected);
    log.info("Actual: " + actual);

    assertGanttSumListEquals(expected, actual);

    markdownTable.append("| ").append(testCounter).append(" | ")
        .append(testName).append(" | ")
        .append(String.format("%.1f", executionTime))
        .append(" |\n");
    testCounter++;

    markdownQueryTable.append("| ").append(testCounter-1).append(" | ")
        .append(testName).append(" | ")
        .append("`").append(formatQueryForMarkdown(query)).append("`").append(" |\n");
  }

  private void assertGanttSumMapEquals(List<GanttColumnSum> expected, List<GanttColumnSum> actual) {
    expected.forEach(exp -> Assertions.assertEquals(exp.getValue(), actual.stream()
        .filter(f -> f.getKey().equalsIgnoreCase(exp.getKey()))
        .findFirst()
        .orElseThrow()
        .getValue(), 0.001)); // Using delta for double comparison
  }

  private void assertGanttSumListEquals(List<GanttColumnSum> expected, List<GanttColumnSum> actual) {
    Assertions.assertEquals(expected.size(), actual.size(), "Lists should have same size");

    Map<String, Double> expectedMap = new HashMap<>();
    Map<String, Double> actualMap = new HashMap<>();

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

  private List<GanttColumnSum> getGanttSumDataActual(String firstColName, String secondColName, CompositeFilter compositeFilter)
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    long first = dStore.getFirst(tProfile.getTableName(), Long.MIN_VALUE, Long.MAX_VALUE);
    long last = dStore.getLast(tProfile.getTableName(), Long.MIN_VALUE, Long.MAX_VALUE);
    return getGanttSumDataActual(firstColName, secondColName, compositeFilter, first, last);
  }

  private List<GanttColumnSum> getGanttSumDataActual(String firstColName, String secondColName, CompositeFilter compositeFilter, long begin, long end)
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    CProfile firstLevelGroupBy = cProfiles.stream()
        .filter(k -> k.getColName().equalsIgnoreCase(firstColName))
        .findAny().get();
    CProfile secondLevelGroupBy = cProfiles.stream()
        .filter(k -> k.getColName().equalsIgnoreCase(secondColName))
        .findAny().get();
    return dStore.getGanttSum(tProfile.getTableName(), firstLevelGroupBy, secondLevelGroupBy, compositeFilter, begin, end);
  }

  private List<GanttColumnSum> getGanttSumDataExpected(String fileName) throws IOException {
    return objectMapper.readValue(getGanttSumTestData(fileName), new TypeReference<>() {});
  }

  private List<GanttColumnSum> getGanttSumDataActual(String firstColName, String secondColName)
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    long first = dStore.getFirst(tProfile.getTableName(), Long.MIN_VALUE, Long.MAX_VALUE);
    long last = dStore.getLast(tProfile.getTableName(), Long.MIN_VALUE, Long.MAX_VALUE);
    return getGanttSumDataActual(firstColName, secondColName, first, last);
  }

  private List<GanttColumnSum> getGanttSumDataActual(String firstColName, String secondColName, long begin, long end)
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    CProfile firstLevelGroupBy = cProfiles.stream()
        .filter(k -> k.getColName().equalsIgnoreCase(firstColName))
        .findAny().get();
    CProfile secondLevelGroupBy = cProfiles.stream()
        .filter(k -> k.getColName().equalsIgnoreCase(secondColName))
        .findAny().get();
    return dStore.getGanttSum(tProfile.getTableName(), firstLevelGroupBy, secondLevelGroupBy, null, begin, end);
  }

  private String getGanttSumTestData(String fileName) throws IOException {
    return Files.readString(Paths.get("src","test", "resources", "json", "gantt-sum", fileName));
  }

  @AfterAll
  public void closeDb() {
    berkleyDB.closeDatabase();

    log.info("\nResults Table:\n" + markdownTable);
    log.info("\nQueries Table:\n" + markdownQueryTable);
  }
}