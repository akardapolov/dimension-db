package ru.dimension.db.integration.sqlite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
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
import ru.dimension.db.model.GroupFunction;
import ru.dimension.db.model.output.StackedColumn;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.SProfile;
import ru.dimension.db.model.profile.TProfile;
import ru.dimension.db.model.profile.table.BType;
import ru.dimension.db.sql.BatchResultSet;

@Log4j2
@TestInstance(Lifecycle.PER_CLASS)
@Disabled
public class DBaseSQLiteRSBackendTest extends AbstractBackendSQLTest {

  private final String dbUrl = "jdbc:sqlite:target/sqlite-datetime-integration.db";
  private final String tableName = "metrics_dt_integration";
  private final String tsName = "ts";
  private BasicDataSource ds;
  private TProfile tProfile;
  private long beginTs;
  private long endTs;

  @BeforeAll
  public void setUp() throws SQLException, TableNameEmptyException {
    BType bType = BType.SQLITE;
    this.ds = getDatasource(bType, "org.sqlite.JDBC", dbUrl, null, null);

    try (Connection conn = ds.getConnection(); Statement st = conn.createStatement()) {
      st.executeUpdate("DROP TABLE IF EXISTS " + tableName);
      st.executeUpdate("CREATE TABLE " + tableName + " (ts DATETIME, cpu REAL)");
    }

    beginTs = Timestamp.valueOf("2023-10-10 10:00:00").getTime();

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

    String insertQuery = "INSERT INTO " + tableName + " VALUES (?, ?)";

    try (Connection conn = ds.getConnection();
        PreparedStatement ps = conn.prepareStatement(insertQuery)) {
      for (int i = 0; i < 5; i++) {
        Timestamp rowTs = new Timestamp(beginTs + (i * 60000L));
        ps.setString(1, sdf.format(rowTs));
        ps.setDouble(2, 10.0 * (i + 1));
        ps.executeUpdate();
      }
      endTs = beginTs + (10 * 60000L);
    }

    initMetaDataBackend(BType.SQLITE, ds);
    SProfile sProfile = getSProfileForBackend(
        tableName, ds, BType.SQLITE,
        "SELECT * FROM " + tableName + " LIMIT 1",
        tsName);

    try (Connection conn = ds.getConnection()) {
      tProfile = dStore.loadJdbcTableMetadata(
          conn,
          "SELECT * FROM " + tableName + " LIMIT 1",
          sProfile);
    }
  }

  @Test
  public void testStackedSum()
      throws BeginEndWrongOrderException, SqlColMetadataException {
    CProfile cpuProfile = tProfile.getCProfiles().stream()
        .filter(c -> c.getColName().equalsIgnoreCase("cpu"))
        .findFirst()
        .orElseThrow();

    List<StackedColumn> stacked = dStore.getStacked(
        tableName, cpuProfile, GroupFunction.SUM,
        null, beginTs, endTs);

    assertFalse(stacked.isEmpty());
  }

  @Test
  public void testBatchResultSetRaw() {
    BatchResultSet rs = dStore.getBatchResultSet(tableName, beginTs, endTs, 2);

    List<Long> actualTs = new ArrayList<>();
    List<Double> actualCpu = new ArrayList<>();

    while (rs.next()) {
      List<List<Object>> batch = rs.getObject();
      for (List<Object> row : batch) {
        actualTs.add(toMillis(row.getFirst()));
        actualCpu.add(((Number) row.get(1)).doubleValue());
      }
    }

    assertEquals(List.of(
        beginTs,
        beginTs + 60000L,
        beginTs + 120000L,
        beginTs + 180000L,
        beginTs + 240000L
    ), actualTs);

    assertEquals(List.of(10.0, 20.0, 30.0, 40.0, 50.0), actualCpu);
  }

  private long toMillis(Object value) {
    if (value instanceof Timestamp ts) {
      return ts.getTime();
    }
    if (value instanceof Number n) {
      return n.longValue();
    }
    if (value instanceof String s) {
      try {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf.parse(s).getTime();
      } catch (Exception e) {
        try {
          return Long.parseLong(s);
        } catch (NumberFormatException ex) {
          throw new AssertionError(
              "Cannot parse ts string: " + s, ex);
        }
      }
    }
    throw new AssertionError(
        "Unsupported ts value type: "
            + value.getClass().getName()
            + ", value=" + value);
  }

  @AfterAll
  public void tearDown() throws SQLException {
    if (this.ds != null) {
      this.ds.close();
    }
  }
}