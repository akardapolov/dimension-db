package ru.dimension.db.integration.duckdb;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
import ru.dimension.db.model.CompareFunction;
import ru.dimension.db.model.GroupFunction;
import ru.dimension.db.model.OrderBy;
import ru.dimension.db.model.filter.CompositeFilter;
import ru.dimension.db.model.filter.FilterCondition;
import ru.dimension.db.model.filter.LogicalOperator;
import ru.dimension.db.model.output.StackedColumn;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.SProfile;
import ru.dimension.db.model.profile.TProfile;
import ru.dimension.db.model.profile.table.BType;
import ru.dimension.db.sql.BatchResultSet;

@Log4j2
@TestInstance(Lifecycle.PER_CLASS)
@Disabled
public class DBaseDuckDBRSBackendTest extends AbstractBackendSQLTest {

  protected String dbUrl;
  private final String driverClassName = "org.duckdb.DuckDBDriver";
  private final String tableName = "duckdb_data_rs";
  private final String tsName = "duckdb_dt_timestamp";
  private final String select = "select * from " + tableName + " limit 2";

  private BasicDataSource ds;

  String createTableRs = """
      CREATE TABLE IF NOT EXISTS duckdb_data_rs (
          duckdb_dt_dec   DOUBLE,
          duckdb_dt_int   INTEGER,
          duckdb_dt_char  VARCHAR,
          duckdb_dt_char_empty VARCHAR,
          duckdb_dt_clob  VARCHAR,
          duckdb_dt_date  DATE,
          duckdb_dt_timestamp TIMESTAMP
      )
      """;

  private SProfile sProfile;
  private TProfile tProfile;
  private long beginTs;
  private long endTs;

  @BeforeAll
  public void setUp() throws SQLException, TableNameEmptyException {
    // TODO File based DuckDB - The process cannot access the file because it is being used by another process
    // dbUrl = "jdbc:duckdb:" + databaseDir.getAbsolutePath() + "/test.duckdb";

    // Memory based DuckDB
    dbUrl = "jdbc:duckdb::memory:testdb";

    BType bType = BType.DUCKDB;
    this.ds = getDatasource(bType, driverClassName, dbUrl, null, null);

    try (Connection conn = ds.getConnection()) {
      if (tableExists(conn, tableName)) {
        dropTable(conn, tableName);
      }
    }

    try (Connection conn = ds.getConnection();
        Statement st = conn.createStatement()) {
      st.executeUpdate(createTableRs);
    }

    java.sql.Date date = java.sql.Date.valueOf("2023-10-10");
    double floatVal = 123.45;
    String clob = "B";
    int number = 12345;
    String varchar2 = "Sample VARCHAR2";

    java.sql.Timestamp ts0 = java.sql.Timestamp.valueOf("2023-10-10 12:00:00");
    java.sql.Timestamp ts1 = java.sql.Timestamp.valueOf("2023-10-11 12:00:00");
    java.sql.Timestamp ts2 = java.sql.Timestamp.valueOf("2023-10-12 12:00:00");
    java.sql.Timestamp ts3 = java.sql.Timestamp.valueOf("2023-10-13 12:00:00");
    beginTs = ts0.getTime();
    endTs = java.sql.Timestamp.valueOf("2023-10-14 00:00:00").getTime();

    String insertQuery = "INSERT INTO duckdb_data_rs VALUES (?, ?, ?, ?, ?, ?, ?)";
    java.sql.Timestamp[] timestamps = {ts0, ts1, ts2, ts3};

    try (PreparedStatement ps = ds.getConnection().prepareStatement(insertQuery)) {
      for (java.sql.Timestamp ts : timestamps) {
        ps.setDouble(1, floatVal);
        ps.setInt(2, number);
        ps.setString(3, varchar2);
        ps.setString(4, ts.equals(ts3) ? "Test" : "");
        ps.setString(5, clob);
        ps.setDate(6, date);
        ps.setTimestamp(7, ts);
        ps.executeUpdate();
      }
    }

    initMetaDataBackend(bType, ds);
    sProfile = getSProfileForBackend(tableName, ds, bType, select, tsName);

    try (Connection conn = ds.getConnection()) {
      tProfile = dStore.loadJdbcTableMetadata(conn, select, sProfile);
    }
    log.info(tProfile);
  }

  @Test
  public void getDistinctTest() throws BeginEndWrongOrderException {
    CProfile cProfile = getCProfileByName(tProfile, "duckdb_dt_char");
    CProfile cProfileEmpty = getCProfileByName(tProfile, "duckdb_dt_char_empty");

    List<String> listActual = dStore.getDistinct(tableName, cProfile, OrderBy.DESC, null, 100, beginTs, endTs);
    List<String> listEmptyActual = dStore.getDistinct(tableName, cProfileEmpty, OrderBy.DESC, null, 100, beginTs, endTs);

    assertEquals(List.of("Sample VARCHAR2"), listActual);
    Collections.sort(listEmptyActual); // Sort for stable assertion
    assertEquals(Arrays.asList("", "Test"), listEmptyActual);
  }

  @Test
  public void getDistinctTestWithFilter() throws BeginEndWrongOrderException {
    CProfile numberProfile = getCProfileByName(tProfile, "duckdb_dt_int");
    CProfile varchar2Profile = getCProfileByName(tProfile, "duckdb_dt_char");
    CProfile clobProfile = getCProfileByName(tProfile, "duckdb_dt_clob");
    CProfile emptyProfile = getCProfileByName(tProfile, "duckdb_dt_char_empty");

    assertDistinctResults(numberProfile, emptyProfile, new String[]{"Test"}, CompareFunction.EQUAL, List.of("12345"));
    assertDistinctResults(varchar2Profile, emptyProfile, new String[]{"Test"}, CompareFunction.EQUAL, List.of("Sample VARCHAR2"));
    assertDistinctResults(varchar2Profile, emptyProfile, new String[]{"Test", ""}, CompareFunction.EQUAL, List.of("Sample VARCHAR2"));
    assertDistinctResults(varchar2Profile, varchar2Profile, new String[]{"VAR"}, CompareFunction.CONTAIN, List.of("Sample VARCHAR2"));
    assertDistinctResults(varchar2Profile, emptyProfile, new String[]{"NonExisting"}, CompareFunction.EQUAL, List.of());
    assertDistinctResults(varchar2Profile, null, null, CompareFunction.EQUAL, List.of("Sample VARCHAR2"));
    assertDistinctResults(clobProfile, emptyProfile, new String[]{"Test"}, CompareFunction.EQUAL, List.of("B"));
  }

  private void assertDistinctResults(CProfile target, CProfile filterProfile, String[] filterValues,
                                     CompareFunction compareFunction, List<String> expected) throws BeginEndWrongOrderException {
    CompositeFilter filter = (filterProfile == null || filterValues == null) ? null :
        new CompositeFilter(List.of(new FilterCondition(filterProfile, filterValues, compareFunction)), LogicalOperator.AND);
    List<String> actual = dStore.getDistinct(tableName, target, OrderBy.ASC, filter, 100, beginTs, endTs);
    assertEquals(expected, actual);
  }

  @Test
  public void batchResultTest() {
    for (int j = 1; j <= 4; j++) {
      BatchResultSet rs = dStore.getBatchResultSet(tableName, beginTs, endTs, j);
      int size = 0;
      if (rs.next()) {
        size = rs.getObject().size();
      }
      assertEquals(j, size);
    }
  }

  @Test
  public void batchResultSingleTest() {
    BatchResultSet rs = dStore.getBatchResultSet(tableName, beginTs, endTs, 2);
    int count = 0;
    while (rs.next()) {
      List<List<Object>> batch = rs.getObject(); // Add this line
      log.info("Fetched batch of size: {}", batch.size());
      count++;
    }
    assertEquals(2, count);
  }

  @Test
  public void bugEmptyValueTest() throws BeginEndWrongOrderException, SqlColMetadataException {
    CProfile cProfile = getCProfileByName(tProfile, "duckdb_dt_char_empty");
    List<StackedColumn> stacked = dStore.getStacked(tableName, cProfile, GroupFunction.COUNT, null, beginTs, endTs);
    assertEquals(3, stacked.get(0).getKeyCount().get(""));
    assertEquals(1, stacked.get(0).getKeyCount().get("Test"));
  }

  @AfterAll
  public void tearDown() throws SQLException {
    if (this.ds != null) {
      // Closing the DataSource is the correct and sufficient way to release all resources.
      // It will close all underlying physical connections. When the last connection
      // to the embedded DuckDB is closed, the driver automatically handles the
      // shutdown and releases the file locks.
      // The explicit "SHUTDOWN" command was interfering with the connection pool's
      // clean-up process.
      this.ds.close();
      log.info("DuckDB DataSource closed.");
    }
  }
}
