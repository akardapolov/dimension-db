package ru.dimension.db.integration.firebird;

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
public class DBaseFirebirdRSBackendTest extends AbstractBackendSQLTest {

  protected String dbUrl;
  private final String driverClassName = "org.firebirdsql.jdbc.FBDriver";

  // Firebird tables/columns are case-insensitive and stored as uppercase.
  private final String tableName = "FIREBIRD_DATA_RS";
  private final String tsName = "FB_DT_TIMESTAMP";

  // Firebird uses 'FIRST N' syntax instead of 'LIMIT'
  private final String select = "select first 2 * from " + tableName;

  private BasicDataSource ds;

  String createTableRs = """
      CREATE TABLE FIREBIRD_DATA_RS (
          FB_DT_DEC   DOUBLE PRECISION,
          FB_DT_INT   INTEGER,
          FB_DT_CHAR  VARCHAR(255),
          FB_DT_CHAR_EMPTY VARCHAR(255),
          FB_DT_CLOB  VARCHAR(4000),
          FB_DT_DATE  DATE,
          FB_DT_TIMESTAMP TIMESTAMP,
          FB_DT_TSTZ TIMESTAMP WITH TIME ZONE
      )
      """;

  private SProfile sProfile;
  private TProfile tProfile;
  private long beginTs;
  private long endTs;

  @BeforeAll
  public void setUp() throws SQLException, TableNameEmptyException {
    dbUrl = "jdbc:firebirdsql://localhost:3050//firebird/data/testrs.fdb?charSet=UTF-8";

    BType bType = BType.FIREBIRD;
    this.ds = getDatasource(bType, driverClassName, dbUrl, "SYSDBA", "masterkey");

    try (Connection conn = ds.getConnection()) {
      dropTableFirebird(conn, tableName);
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

    String insertQuery = "INSERT INTO FIREBIRD_DATA_RS VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)";
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
    CProfile cProfile = getCProfileByName(tProfile, "FB_DT_CHAR");
    CProfile cProfileEmpty = getCProfileByName(tProfile, "FB_DT_CHAR_EMPTY");

    List<String> listActual = dStore.getDistinct(tableName, cProfile, OrderBy.DESC, null, 100, beginTs, endTs);
    List<String> listEmptyActual = dStore.getDistinct(tableName, cProfileEmpty, OrderBy.DESC, null, 100, beginTs, endTs);

    assertEquals(List.of("Sample VARCHAR2"), listActual);
    Collections.sort(listEmptyActual); // Sort for stable assertion
    assertEquals(Arrays.asList("", "Test"), listEmptyActual);
  }

  @Test
  public void getDistinctTestWithFilter() throws BeginEndWrongOrderException {
    CProfile numberProfile = getCProfileByName(tProfile, "FB_DT_INT");
    CProfile varchar2Profile = getCProfileByName(tProfile, "FB_DT_CHAR");
    CProfile clobProfile = getCProfileByName(tProfile, "FB_DT_CLOB");
    CProfile emptyProfile = getCProfileByName(tProfile, "FB_DT_CHAR_EMPTY");

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
      List<List<Object>> batch = rs.getObject();
      log.info("Fetched batch of size: {}", batch.size());
      count++;
    }
    assertEquals(2, count);
  }

  @Test
  public void bugEmptyValueTest() throws BeginEndWrongOrderException, SqlColMetadataException {
    CProfile cProfile = getCProfileByName(tProfile, "FB_DT_CHAR_EMPTY");
    List<StackedColumn> stacked = dStore.getStacked(tableName, cProfile, GroupFunction.COUNT, null, beginTs, endTs);
    assertEquals(3, stacked.get(0).getKeyCount().get(""));
    assertEquals(1, stacked.get(0).getKeyCount().get("Test"));
  }

  @AfterAll
  public void tearDown() throws SQLException {
    if (this.ds != null) {
      this.ds.close();
      log.info("Firebird DataSource closed.");
    }
  }
}