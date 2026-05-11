package ru.dimension.db.integration.pqsql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import ru.dimension.db.common.AbstractBackendSQLTest;
import ru.dimension.db.exception.BeginEndWrongOrderException;
import ru.dimension.db.exception.SqlColMetadataException;
import ru.dimension.db.exception.TableNameEmptyException;
import ru.dimension.db.model.GranularityFunction;
import ru.dimension.db.model.GroupFunction;
import ru.dimension.db.model.PercentileFunction;
import ru.dimension.db.model.output.StackedColumn;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.SProfile;
import ru.dimension.db.model.profile.TProfile;
import ru.dimension.db.model.profile.table.BType;

@Log4j2
@TestInstance(Lifecycle.PER_CLASS)
@Disabled
public class DBasePgSQLPercentileTest extends AbstractBackendSQLTest {

  private static final long MINUTE_MS = 60_000L;

  private final String dbUrl = "jdbc:postgresql://localhost:5432/postgres";
  private final String driverClassName = "org.postgresql.Driver";
  private final String tableName = "pg_percentile_data";
  private final String tsName = "ts";

  private BasicDataSource ds;
  private TProfile tProfile;
  private long rangeBegin;
  private long rangeEnd;

  @BeforeAll
  public void setUp() throws SQLException, TableNameEmptyException {
    BType bType = BType.POSTGRES;
    this.ds = getDatasource(bType, driverClassName, dbUrl, "postgres", "postgres");

    try (Connection conn = ds.getConnection(); Statement st = conn.createStatement()) {
      st.executeUpdate("DROP TABLE IF EXISTS " + tableName);
      st.executeUpdate("CREATE TABLE " + tableName +
          " (ts TIMESTAMP, long_field BIGINT, string_field VARCHAR(10))");
    }

    rangeBegin = Timestamp.valueOf("2024-01-01 10:00:00").getTime();
    rangeEnd = rangeBegin + 10 * MINUTE_MS;

    try (Connection conn = ds.getConnection();
         PreparedStatement ps = conn.prepareStatement(
             "INSERT INTO " + tableName + " VALUES (?, ?, ?)")) {
      for (int i = 0; i < 10; i++) {
        ps.setTimestamp(1, new Timestamp(rangeBegin + i * MINUTE_MS));
        ps.setLong(2, i + 1L);
        ps.setString(3, (i % 2 == 0) ? "A" : "B");
        ps.executeUpdate();
      }
    }

    initMetaDataBackend(bType, ds);
    SProfile sProfile = getSProfileForBackend(
        tableName, ds, bType,
        "SELECT * FROM " + tableName + " LIMIT 1",
        tsName);

    try (Connection conn = ds.getConnection()) {
      tProfile = dStore.loadJdbcTableMetadata(
          conn,
          "SELECT * FROM " + tableName + " LIMIT 1",
          sProfile);
    }
  }

  private CProfile profile(String colName) {
    return tProfile.getCProfiles().stream()
        .filter(c -> c.getColName().equalsIgnoreCase(colName))
        .findAny()
        .orElseThrow();
  }

  @Test
  public void avgWithP50ReturnsMedian() throws BeginEndWrongOrderException, SqlColMetadataException {
    List<StackedColumn> result = dStore.getStacked(tableName,
                                                   profile("long_field"),
                                                   GroupFunction.AVG,
                                                   PercentileFunction.P50,
                                                   GranularityFunction.AUTO,
                                                   null,
                                                   rangeBegin, rangeEnd);
    assertEquals(1, result.size());
    Map<String, Double> percentile = result.getFirst().getKeyPercentile();
    assertNotNull(percentile);
    assertEquals(5.0, percentile.get("LONG_FIELD"));
  }

  @Test
  public void avgWithP90ReturnsExpected() throws BeginEndWrongOrderException, SqlColMetadataException {
    List<StackedColumn> result = dStore.getStacked(tableName,
                                                   profile("long_field"),
                                                   GroupFunction.AVG,
                                                   PercentileFunction.P90,
                                                   GranularityFunction.AUTO,
                                                   null,
                                                   rangeBegin, rangeEnd);
    assertEquals(9.0, result.getFirst().getKeyPercentile().get("LONG_FIELD"));
  }

  @Test
  public void sumWithP95ReturnsExpected() throws BeginEndWrongOrderException, SqlColMetadataException {
    List<StackedColumn> result = dStore.getStacked(tableName,
                                                   profile("long_field"),
                                                   GroupFunction.SUM,
                                                   PercentileFunction.P95,
                                                   GranularityFunction.AUTO,
                                                   null,
                                                   rangeBegin, rangeEnd);
    assertEquals(10.0, result.getFirst().getKeyPercentile().get("LONG_FIELD"));
  }

  @Test
  public void countWithP50MinuteGranularityProducesPercentilePerGroup()
      throws BeginEndWrongOrderException, SqlColMetadataException {
    List<StackedColumn> result = dStore.getStacked(tableName,
                                                   profile("string_field"),
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
  public void countWithP90HourGranularityFallsBack()
      throws BeginEndWrongOrderException, SqlColMetadataException {
    List<StackedColumn> result = dStore.getStacked(tableName,
                                                   profile("string_field"),
                                                   GroupFunction.COUNT,
                                                   PercentileFunction.P90,
                                                   GranularityFunction.HOUR,
                                                   null,
                                                   rangeBegin, rangeEnd);
    assertTrue(result.stream()
                   .anyMatch(c -> c.getKeyCount() != null && !c.getKeyCount().isEmpty()),
               "Expected fallback to populate keyCount when buckets < 5");
    assertTrue(result.stream()
                   .allMatch(c -> c.getKeyPercentile() == null || c.getKeyPercentile().isEmpty()),
               "Expected keyPercentile to remain empty in fallback path");
  }

  @Test
  public void noneDelegatesToRegularGetStacked()
      throws BeginEndWrongOrderException, SqlColMetadataException {
    List<StackedColumn> percentileNone = dStore.getStacked(tableName,
                                                           profile("string_field"),
                                                           GroupFunction.COUNT,
                                                           PercentileFunction.NONE,
                                                           GranularityFunction.AUTO,
                                                           null,
                                                           rangeBegin, rangeEnd);

    List<StackedColumn> regular = dStore.getStacked(tableName,
                                                    profile("string_field"),
                                                    GroupFunction.COUNT,
                                                    null,
                                                    rangeBegin, rangeEnd);

    assertEquals(regular.size(), percentileNone.size());
    for (int i = 0; i < regular.size(); i++) {
      assertEquals(regular.get(i).getKeyCount(), percentileNone.get(i).getKeyCount());
    }
  }

  @AfterAll
  public void tearDown() throws SQLException {
    if (ds != null) {
      try (Connection conn = ds.getConnection(); Statement st = conn.createStatement()) {
        st.executeUpdate("DROP TABLE IF EXISTS " + tableName);
      }
      ds.close();
    }
  }
}
