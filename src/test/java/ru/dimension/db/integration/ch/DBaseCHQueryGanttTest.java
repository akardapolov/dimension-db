package ru.dimension.db.integration.ch;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import lombok.extern.log4j.Log4j2;
import ru.dimension.db.DBase;
import ru.dimension.db.backend.BerkleyDB;
import ru.dimension.db.config.DBaseConfig;
import ru.dimension.db.core.DStore;
import ru.dimension.db.exception.BeginEndWrongOrderException;
import ru.dimension.db.exception.GanttColumnNotSupportedException;
import ru.dimension.db.exception.SqlColMetadataException;
import ru.dimension.db.exception.TableNameEmptyException;
import ru.dimension.db.model.output.GanttColumnCount;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.TProfile;
import ru.dimension.db.source.ClickHouse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@Log4j2
@TestInstance(Lifecycle.PER_CLASS)
@Disabled
public class DBaseCHQueryGanttTest implements ClickHouse {
  private static final StringBuilder markdownTable = new StringBuilder();
  private static final StringBuilder markdownQueryTable = new StringBuilder();
  private int testCounter = 1;
  private int batchSize = 2;

  static {
    // Main results table - updated to single execution time column
    markdownTable.append("| № | Test name | Execution time (single/4-thread) (sec) |\n");
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
  public void getGanttRawRaw()
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException, IOException {
    String testName = "getGanttRawRaw";
    String query = """
                 SELECT pickup_cdeligibil, vendor_id, COUNT(vendor_id)
                 FROM datasets.trips_mergetree
                 WHERE toYear(pickup_date) = 2016
                 GROUP BY pickup_cdeligibil, vendor_id;
            """;
    log.info("Query: " + "\n" + query);

    List<GanttColumnCount> expected = getGanttDataExpected("pickup_cdeligibil__vendor_id.json");

    double executionTimeSingle;
    double executionTimeParallel;
    List<GanttColumnCount> actual;
    List<GanttColumnCount> actualParallel;

    // Single-threaded execution
    Instant startSingle = Instant.now();
    try {
      actual = getGanttDataActual("PICKUP_CDELIGIBIL", "VENDOR_ID");
    } finally {
      Instant endSingle = Instant.now();
      executionTimeSingle = Duration.between(startSingle, endSingle).toMillis() / 1000.0;
    }

    // Parallel execution (4 threads)
    Instant startParallel = Instant.now();
    try {
      actualParallel = getGanttDataActual("PICKUP_CDELIGIBIL", "VENDOR_ID", batchSize);
    } finally {
      Instant endParallel = Instant.now();
      executionTimeParallel = Duration.between(startParallel, endParallel).toMillis() / 1000.0;
    }

    log.info("Expected: " + expected);
    log.info("Actual: " + actual);
    log.info("Actual parallel: " + actualParallel);

    assertGanttListEquals(expected, actual);
    assertGanttMapEquals(expected, actual);

    assertGanttListEquals(expected, actualParallel);
    assertGanttMapEquals(expected, actualParallel);

    markdownTable.append("| ").append(testCounter).append(" | ")
        .append(testName).append(" | ")
        .append(String.format("%.1f / %.1f", executionTimeSingle, executionTimeParallel))
        .append(" |\n");
    testCounter++;

    markdownQueryTable.append("| ").append(testCounter-1).append(" | ")
        .append(testName).append(" | ")
        .append("`").append(formatQueryForMarkdown(query)).append("`").append(" |\n");
  }

  @Test
  public void getGanttEnumEnum()
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException, IOException {
    String testName = "getGanttEnumEnum";
    String query = """
                 select dropoff_puma, dropoff_borocode, COUNT(dropoff_borocode)
                 from datasets.trips_mergetree
                 where toYear(pickup_date) = 2016
                 group by dropoff_puma, dropoff_borocode;
            """;
    log.info("Query: " + "\n" + query);

    List<GanttColumnCount> expected = getGanttDataExpected("dropoff_puma__dropoff_borocode.json");

    double executionTimeSingle;
    double executionTimeParallel;
    List<GanttColumnCount> actual;
    List<GanttColumnCount> actualParallel;

    // Single-threaded execution
    Instant startSingle = Instant.now();
    try {
      actual = getGanttDataActual("DROPOFF_PUMA", "DROPOFF_BOROCODE");
    } finally {
      Instant endSingle = Instant.now();
      executionTimeSingle = Duration.between(startSingle, endSingle).toMillis() / 1000.0;
    }

    // Parallel execution (4 threads)
    Instant startParallel = Instant.now();
    try {
      actualParallel = getGanttDataActual("DROPOFF_PUMA", "DROPOFF_BOROCODE", batchSize);
    } finally {
      Instant endParallel = Instant.now();
      executionTimeParallel = Duration.between(startParallel, endParallel).toMillis() / 1000.0;
    }

    log.info("Expected: " + expected);
    log.info("Actual: " + actual);
    log.info("Actual parallel: " + actualParallel);

    assertGanttListEquals(expected, actual);
    assertGanttMapEquals(expected, actual);

    assertGanttListEquals(expected, actualParallel);
    assertGanttMapEquals(expected, actualParallel);

    markdownTable.append("| ").append(testCounter).append(" | ")
        .append(testName).append(" | ")
        .append(String.format("%.1f / %.1f", executionTimeSingle, executionTimeParallel))
        .append(" |\n");
    testCounter++;

    markdownQueryTable.append("| ").append(testCounter-1).append(" | ")
        .append(testName).append(" | ")
        .append("`").append(formatQueryForMarkdown(query)).append("`").append(" |\n");
  }

  @Test
  public void getGanttHistHist()
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException, IOException {
    String testName = "getGanttHistHist";
    String query = """
                      SELECT trip_type, pickup_boroname, COUNT(pickup_boroname)
                      FROM datasets.trips_mergetree
                      WHERE toYear(pickup_date) = 2016
                      GROUP BY trip_type, pickup_boroname;
            """;
    log.info("Query: " + "\n" + query);

    List<GanttColumnCount> expected = getGanttDataExpected("trip_type__pickup_boroname.json");

    double executionTimeSingle;
    double executionTimeParallel;
    List<GanttColumnCount> actual;
    List<GanttColumnCount> actualParallel;

    // Single-threaded execution
    Instant startSingle = Instant.now();
    try {
      actual = getGanttDataActual("TRIP_TYPE", "PICKUP_BORONAME");
    } finally {
      Instant endSingle = Instant.now();
      executionTimeSingle = Duration.between(startSingle, endSingle).toMillis() / 1000.0;
    }

    // Parallel execution (4 threads)
    Instant startParallel = Instant.now();
    try {
      actualParallel = getGanttDataActual("TRIP_TYPE", "PICKUP_BORONAME", batchSize);
    } finally {
      Instant endParallel = Instant.now();
      executionTimeParallel = Duration.between(startParallel, endParallel).toMillis() / 1000.0;
    }

    log.info("Expected: " + expected);
    log.info("Actual: " + actual);
    log.info("Actual parallel: " + actualParallel);

    assertGanttListEquals(expected, actual);
    assertGanttMapEquals(expected, actual);

    assertGanttListEquals(expected, actualParallel);
    assertGanttMapEquals(expected, actualParallel);

    markdownTable.append("| ").append(testCounter).append(" | ")
        .append(testName).append(" | ")
        .append(String.format("%.1f / %.1f", executionTimeSingle, executionTimeParallel))
        .append(" |\n");
    testCounter++;

    markdownQueryTable.append("| ").append(testCounter-1).append(" | ")
        .append(testName).append(" | ")
        .append("`").append(formatQueryForMarkdown(query)).append("`").append(" |\n");
  }

  @Test
  public void getGanttHistRaw()
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException, IOException {
    String testName = "getGanttHistRaw";
    String query = """
                 SELECT trip_type, vendor_id, COUNT(vendor_id)
                 FROM datasets.trips_mergetree
                 WHERE toYear(pickup_date) = 2016
                 GROUP BY trip_type, vendor_id;
            """;
    log.info("Query: " + "\n" + query);

    List<GanttColumnCount> expected = getGanttDataExpected("trip_type__vendor_id.json");

    double executionTimeSingle;
    double executionTimeParallel;
    List<GanttColumnCount> actual;
    List<GanttColumnCount> actualParallel;

    // Single-threaded execution
    Instant startSingle = Instant.now();
    try {
      actual = getGanttDataActual("TRIP_TYPE", "VENDOR_ID");
    } finally {
      Instant endSingle = Instant.now();
      executionTimeSingle = Duration.between(startSingle, endSingle).toMillis() / 1000.0;
    }

    // Parallel execution (4 threads)
    Instant startParallel = Instant.now();
    try {
      actualParallel = getGanttDataActual("TRIP_TYPE", "VENDOR_ID", batchSize);
    } finally {
      Instant endParallel = Instant.now();
      executionTimeParallel = Duration.between(startParallel, endParallel).toMillis() / 1000.0;
    }

    log.info("Expected: " + expected);
    log.info("Actual: " + actual);
    log.info("Actual parallel: " + actualParallel);

    assertGanttListEquals(expected, actual);
    assertGanttMapEquals(expected, actual);

    assertGanttListEquals(expected, actualParallel);
    assertGanttMapEquals(expected, actualParallel);

    markdownTable.append("| ").append(testCounter).append(" | ")
        .append(testName).append(" | ")
        .append(String.format("%.1f / %.1f", executionTimeSingle, executionTimeParallel))
        .append(" |\n");
    testCounter++;

    markdownQueryTable.append("| ").append(testCounter-1).append(" | ")
        .append(testName).append(" | ")
        .append("`").append(formatQueryForMarkdown(query)).append("`").append(" |\n");
  }

  @Test
  public void getGanttHistEnum()
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException, IOException {
    String testName = "getGanttHistEnum";
    String query = """
                 SELECT trip_type, dropoff_boroname, COUNT(dropoff_boroname)
                 FROM datasets.trips_mergetree
                 WHERE toYear(pickup_date) = 2016
                 GROUP BY trip_type, dropoff_boroname;
            """;
    log.info("Query: " + "\n" + query);

    List<GanttColumnCount> expected = getGanttDataExpected("trip_type__dropoff_boroname.json");

    double executionTimeSingle;
    double executionTimeParallel;
    List<GanttColumnCount> actual;
    List<GanttColumnCount> actualParallel;

    // Single-threaded execution
    Instant startSingle = Instant.now();
    try {
      actual = getGanttDataActual("TRIP_TYPE", "DROPOFF_BORONAME");
    } finally {
      Instant endSingle = Instant.now();
      executionTimeSingle = Duration.between(startSingle, endSingle).toMillis() / 1000.0;
    }

    // Parallel execution (4 threads)
    Instant startParallel = Instant.now();
    try {
      actualParallel = getGanttDataActual("TRIP_TYPE", "DROPOFF_BORONAME", batchSize);
    } finally {
      Instant endParallel = Instant.now();
      executionTimeParallel = Duration.between(startParallel, endParallel).toMillis() / 1000.0;
    }

    log.info("Expected: " + expected);
    log.info("Actual: " + actual);
    log.info("Actual parallel: " + actualParallel);

    assertGanttListEquals(expected, actual);
    assertGanttMapEquals(expected, actual);

    assertGanttListEquals(expected, actualParallel);
    assertGanttMapEquals(expected, actualParallel);

    markdownTable.append("| ").append(testCounter).append(" | ")
        .append(testName).append(" | ")
        .append(String.format("%.1f / %.1f", executionTimeSingle, executionTimeParallel))
        .append(" |\n");
    testCounter++;

    markdownQueryTable.append("| ").append(testCounter-1).append(" | ")
        .append(testName).append(" | ")
        .append("`").append(formatQueryForMarkdown(query)).append("`").append(" |\n");
  }

  @Test
  public void getGanttEnumRaw()
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException, IOException {
    String testName = "getGanttEnumRaw";
    String query = """
                 SELECT dropoff_boroname, vendor_id, COUNT(vendor_id)
                 FROM datasets.trips_mergetree
                 WHERE toYear(pickup_date) = 2016
                 GROUP BY dropoff_boroname, vendor_id;
            """;
    log.info("Query: " + "\n" + query);

    List<GanttColumnCount> expected = getGanttDataExpected("dropoff_boroname__vendor_id.json");

    double executionTimeSingle;
    double executionTimeParallel;
    List<GanttColumnCount> actual;
    List<GanttColumnCount> actualParallel;

    // Single-threaded execution
    Instant startSingle = Instant.now();
    try {
      actual = getGanttDataActual("DROPOFF_BORONAME", "VENDOR_ID");
    } finally {
      Instant endSingle = Instant.now();
      executionTimeSingle = Duration.between(startSingle, endSingle).toMillis() / 1000.0;
    }

    // Parallel execution (4 threads)
    Instant startParallel = Instant.now();
    try {
      actualParallel = getGanttDataActual("DROPOFF_BORONAME", "VENDOR_ID", batchSize);
    } finally {
      Instant endParallel = Instant.now();
      executionTimeParallel = Duration.between(startParallel, endParallel).toMillis() / 1000.0;
    }

    log.info("Expected: " + expected);
    log.info("Actual: " + actual);
    log.info("Actual parallel: " + actualParallel);

    assertGanttListEquals(expected, actual);
    assertGanttMapEquals(expected, actual);

    assertGanttListEquals(expected, actualParallel);
    assertGanttMapEquals(expected, actualParallel);

    markdownTable.append("| ").append(testCounter).append(" | ")
        .append(testName).append(" | ")
        .append(String.format("%.1f / %.1f", executionTimeSingle, executionTimeParallel))
        .append(" |\n");
    testCounter++;

    markdownQueryTable.append("| ").append(testCounter-1).append(" | ")
        .append(testName).append(" | ")
        .append("`").append(formatQueryForMarkdown(query)).append("`").append(" |\n");
  }

  @Test
  public void getGanttEnumHist()
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException, IOException {
    String testName = "getGanttEnumHist";
    String query = """
                 SELECT dropoff_boroname, pickup_boroname, COUNT(pickup_boroname)
                 FROM datasets.trips_mergetree
                 WHERE toYear(pickup_date) = 2016
                 GROUP BY dropoff_boroname, pickup_boroname;
            """;
    log.info("Query: " + "\n" + query);

    List<GanttColumnCount> expected = getGanttDataExpected("dropoff_boroname__pickup_boroname.json");

    double executionTimeSingle;
    double executionTimeParallel;
    List<GanttColumnCount> actual;
    List<GanttColumnCount> actualParallel;

    // Single-threaded execution
    Instant startSingle = Instant.now();
    try {
      actual = getGanttDataActual("DROPOFF_BORONAME", "PICKUP_BORONAME");
    } finally {
      Instant endSingle = Instant.now();
      executionTimeSingle = Duration.between(startSingle, endSingle).toMillis() / 1000.0;
    }

    // Parallel execution (4 threads)
    Instant startParallel = Instant.now();
    try {
      actualParallel = getGanttDataActual("DROPOFF_BORONAME", "PICKUP_BORONAME", batchSize);
    } finally {
      Instant endParallel = Instant.now();
      executionTimeParallel = Duration.between(startParallel, endParallel).toMillis() / 1000.0;
    }

    log.info("Expected: " + expected);
    log.info("Actual: " + actual);
    log.info("Actual parallel: " + actualParallel);

    assertGanttListEquals(expected, actual);
    assertGanttMapEquals(expected, actual);

    assertGanttListEquals(expected, actualParallel);
    assertGanttMapEquals(expected, actualParallel);

    markdownTable.append("| ").append(testCounter).append(" | ")
        .append(testName).append(" | ")
        .append(String.format("%.1f / %.1f", executionTimeSingle, executionTimeParallel))
        .append(" |\n");
    testCounter++;

    markdownQueryTable.append("| ").append(testCounter-1).append(" | ")
        .append(testName).append(" | ")
        .append("`").append(formatQueryForMarkdown(query)).append("`").append(" |\n");
  }

  @Test
  public void getGanttRawHist()
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException, IOException {
    String testName = "getGanttRawHist";
    String query = """
                 SELECT pickup_cdeligibil, pickup_boroname, COUNT(pickup_boroname)
                 FROM datasets.trips_mergetree
                 WHERE toYear(pickup_date) = 2016
                 GROUP BY pickup_cdeligibil, pickup_boroname;
            """;
    log.info("Query: " + "\n" + query);

    List<GanttColumnCount> expected = getGanttDataExpected("pickup_cdeligibil__pickup_boroname.json");

    double executionTimeSingle;
    double executionTimeParallel;
    List<GanttColumnCount> actual;
    List<GanttColumnCount> actualParallel;

    // Single-threaded execution
    Instant startSingle = Instant.now();
    try {
      actual = getGanttDataActual("PICKUP_CDELIGIBIL", "PICKUP_BORONAME");
    } finally {
      Instant endSingle = Instant.now();
      executionTimeSingle = Duration.between(startSingle, endSingle).toMillis() / 1000.0;
    }

    // Parallel execution (4 threads)
    Instant startParallel = Instant.now();
    try {
      actualParallel = getGanttDataActual("PICKUP_CDELIGIBIL", "PICKUP_BORONAME", batchSize);
    } finally {
      Instant endParallel = Instant.now();
      executionTimeParallel = Duration.between(startParallel, endParallel).toMillis() / 1000.0;
    }

    log.info("Expected: " + expected);
    log.info("Actual: " + actual);
    log.info("Actual parallel: " + actualParallel);

    assertGanttListEquals(expected, actual);
    assertGanttMapEquals(expected, actual);

    assertGanttListEquals(expected, actualParallel);
    assertGanttMapEquals(expected, actualParallel);

    markdownTable.append("| ").append(testCounter).append(" | ")
        .append(testName).append(" | ")
        .append(String.format("%.1f / %.1f", executionTimeSingle, executionTimeParallel))
        .append(" |\n");
    testCounter++;

    markdownQueryTable.append("| ").append(testCounter-1).append(" | ")
        .append(testName).append(" | ")
        .append("`").append(formatQueryForMarkdown(query)).append("`").append(" |\n");
  }

  @Test
  public void getGanttRawEnum()
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException, IOException {
    String testName = "getGanttRawEnum";
    String query = """
                 SELECT pickup_cdeligibil, cab_type, COUNT(cab_type)
                 FROM datasets.trips_mergetree
                 WHERE toYear(pickup_date) = 2016
                 GROUP BY pickup_cdeligibil, cab_type;
            """;
    log.info("Query: " + "\n" + query);

    List<GanttColumnCount> expected = getGanttDataExpected("pickup_cdeligibil__cab_type.json");

    double executionTimeSingle;
    double executionTimeParallel;
    List<GanttColumnCount> actual;
    List<GanttColumnCount> actualParallel;

    // Single-threaded execution
    Instant startSingle = Instant.now();
    try {
      actual = getGanttDataActual("PICKUP_CDELIGIBIL", "CAB_TYPE");
    } finally {
      Instant endSingle = Instant.now();
      executionTimeSingle = Duration.between(startSingle, endSingle).toMillis() / 1000.0;
    }

    // Parallel execution (4 threads)
    Instant startParallel = Instant.now();
    try {
      actualParallel = getGanttDataActual("PICKUP_CDELIGIBIL", "CAB_TYPE", 2);
    } finally {
      Instant endParallel = Instant.now();
      executionTimeParallel = Duration.between(startParallel, endParallel).toMillis() / 1000.0;
    }

    log.info("Expected: " + expected);
    log.info("Actual: " + actual);
    log.info("Actual parallel: " + actualParallel);

    assertGanttListEquals(expected, actual);
    assertGanttMapEquals(expected, actual);

    assertGanttListEquals(expected, actualParallel);
    assertGanttMapEquals(expected, actualParallel);

    markdownTable.append("| ").append(testCounter).append(" | ")
        .append(testName).append(" | ")
        .append(String.format("%.1f / %.1f", executionTimeSingle, executionTimeParallel))
        .append(" |\n");
    testCounter++;

    // Fixed the counter to use testCounter-1 for consistent numbering
    markdownQueryTable.append("| ").append(testCounter-1).append(" | ")
        .append(testName).append(" | ")
        .append("`").append(formatQueryForMarkdown(query)).append("`").append(" |\n");
  }

  private void assertGanttMapEquals(List<GanttColumnCount> expected, List<GanttColumnCount> actual) {
    expected.forEach(exp -> Assertions.assertEquals(exp.getGantt(), actual.stream()
        .filter(f -> f.getKey().equalsIgnoreCase(exp.getKey()))
        .findFirst()
        .orElseThrow()
        .getGantt()));
  }

  public void assertGanttListEquals(List<GanttColumnCount> expected, List<GanttColumnCount> actual) {
    assertTrue(expected.size() == actual.size() && expected.containsAll(actual) && actual.containsAll(expected));
  }

  private List<GanttColumnCount> getGanttDataExpected(String fileName) throws IOException {
    return objectMapper.readValue(getGanttTestData(fileName), new TypeReference<>() {});
  }

  private List<GanttColumnCount> getGanttDataActual(String firstColName, String secondColName)
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    long first = dStore.getFirst(tProfile.getTableName(), Long.MIN_VALUE, Long.MAX_VALUE);
    long last = dStore.getLast(tProfile.getTableName(), Long.MIN_VALUE, Long.MAX_VALUE);
    return getGanttDataActual(firstColName, secondColName, first, last);
  }

  private List<GanttColumnCount> getGanttDataActual(String firstColName, String secondColName, int batchSize)
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    long first = dStore.getFirst(tProfile.getTableName(), Long.MIN_VALUE, Long.MAX_VALUE);
    long last = dStore.getLast(tProfile.getTableName(), Long.MIN_VALUE, Long.MAX_VALUE);
    return getGanttDataActual(firstColName, secondColName, batchSize, first, last);
  }

  private List<GanttColumnCount> getGanttDataActual(String firstColName, String secondColName, long begin, long end)
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    CProfile firstLevelGroupBy = cProfiles.stream()
        .filter(k -> k.getColName().equalsIgnoreCase(firstColName))
        .findAny().get();
    CProfile secondLevelGroupBy = cProfiles.stream()
        .filter(k -> k.getColName().equalsIgnoreCase(secondColName))
        .findAny().get();
    return getListGanttColumnTwoLevelGrouping(dStore, firstLevelGroupBy, secondLevelGroupBy, begin, end);
  }

  private List<GanttColumnCount> getGanttDataActual(String firstColName, String secondColName, int batchSize, long begin, long end)
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    CProfile firstLevelGroupBy = cProfiles.stream()
        .filter(k -> k.getColName().equalsIgnoreCase(firstColName))
        .findAny().get();
    CProfile secondLevelGroupBy = cProfiles.stream()
        .filter(k -> k.getColName().equalsIgnoreCase(secondColName))
        .findAny().get();
    return getListGanttColumnTwoLevelGrouping(dStore, firstLevelGroupBy, secondLevelGroupBy, batchSize, begin, end);
  }

  private List<GanttColumnCount> getListGanttColumnTwoLevelGrouping(DStore dStore,
                                                                    CProfile firstLevelGroupBy,
                                                                    CProfile secondLevelGroupBy,
                                                                    long begin,
                                                                    long end)
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    return dStore.getGanttCount(tProfile.getTableName(), firstLevelGroupBy, secondLevelGroupBy, null, begin, end);
  }

  private List<GanttColumnCount> getListGanttColumnTwoLevelGrouping(DStore dStore,
                                                                    CProfile firstLevelGroupBy,
                                                                    CProfile secondLevelGroupBy,
                                                                    int batchSize,
                                                                    long begin,
                                                                    long end)
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    return dStore.getGanttCount(tProfile.getTableName(), firstLevelGroupBy, secondLevelGroupBy, null, batchSize, begin, end);
  }

  private String getGanttTestData(String fileName) throws IOException {
    return Files.readString(Paths.get("src","test", "resources", "json", "gantt", fileName));
  }

  @AfterAll
  public void closeDb() {
    berkleyDB.closeDatabase();

    log.info("\nResults Table:\n" + markdownTable);
    log.info("\nQueries Table:\n" + markdownQueryTable);
  }
}