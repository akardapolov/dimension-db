package ru.dimension.db.integration.mysql;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.dbcp2.BasicDataSource;
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
public class DBaseMySQLRSBackendTest extends AbstractBackendSQLTest {

  protected final String dbUrl = "jdbc:mysql://localhost:3307/dimension-db?createDatabaseIfNotExist=true";
  private final String driverClassName = "com.mysql.cj.jdbc.Driver";
  private final String tableName = "mysql_data_rs";
  private final String tsName = "mysql_dt_timestamp";
  private final String select = "select * from " + tableName + " limit 2";

  String createTableRs = """
      CREATE TABLE IF NOT EXISTS mysql_data_rs (
          mysql_dt_dec   DOUBLE,
          mysql_dt_int   INT,
          mysql_dt_char  VARCHAR(32),
          mysql_dt_char_empty VARCHAR(32),
          mysql_dt_clob  LONGTEXT,
          mysql_dt_date  DATE,
          mysql_dt_timestamp TIMESTAMP(6)
      )
      """;

  private SProfile sProfile;
  private TProfile tProfile;

  @BeforeAll
  public void setUp() throws SQLException, TableNameEmptyException {
    BType bType = BType.MYSQL;
    BasicDataSource ds = getDatasource(bType, driverClassName, dbUrl, "root", "root");

    if (tableExists(ds.getConnection(), tableName)) {
      dropTableMySQL(ds.getConnection(), tableName);
    }

    try (Statement st = ds.getConnection().createStatement()) {
      st.executeUpdate(createTableRs);
    }

    java.sql.Date date = java.sql.Date.valueOf("2023-10-10");
    float floatVal = 123.45f;
    String clob = "B";
    int number = 12345;
    String varchar2 = "Sample VARCHAR2";

    java.sql.Timestamp ts0 = java.sql.Timestamp.valueOf("2023-10-10 12:00:00");
    java.sql.Timestamp ts1 = java.sql.Timestamp.valueOf("2023-10-11 12:00:00");
    java.sql.Timestamp ts2 = java.sql.Timestamp.valueOf("2023-10-12 12:00:00");
    java.sql.Timestamp ts3 = java.sql.Timestamp.valueOf("2023-10-13 12:00:00");

    String insertQuery = """
        INSERT INTO mysql_data_rs
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """;

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
    tProfile = dStore.loadJdbcTableMetadata(ds.getConnection(), select, sProfile);

    log.info(tProfile);
  }

  @Test
  public void getDistinctTest() throws BeginEndWrongOrderException {
    log.info(tProfile);

    CProfile cProfile = getCProfileByName(tProfile, "mysql_dt_char");
    CProfile cProfileEmpty = getCProfileByName(tProfile, "mysql_dt_char_empty");

    List<String> listActual = dStore.getDistinct(tableName, cProfile, OrderBy.DESC, null, 100, 0L, 1697357130000L);
    List<String> listEmptyActual = dStore.getDistinct(tableName, cProfileEmpty, OrderBy.DESC, null, 100, 0L, 1697357130000L);

    assertEquals(List.of("Sample VARCHAR2"), listActual);
    assertEquals(Arrays.asList("Test", ""), listEmptyActual);
  }

  @Test
  public void getDistinctTestWithFilter() throws BeginEndWrongOrderException {
    CProfile numberProfile = getCProfileByName(tProfile, "mysql_dt_int");
    CProfile varchar2Profile = getCProfileByName(tProfile, "mysql_dt_char");
    CProfile clobProfile = getCProfileByName(tProfile, "mysql_dt_clob");
    CProfile emptyProfile = getCProfileByName(tProfile, "mysql_dt_char_empty");

    String[] filterValues = {"Test"};
    String[] multiFilterValues = {"Test", ""};
    String[] varCharFilter = {"VAR"};
    String[] nonExistingFilter = {"NonExisting"};

    BatchResultSet batchResultSet = dStore.getBatchResultSet(tableName, 0L, 1697357130000L, 10);
    while (batchResultSet.next()) {
      List<List<Object>> var = batchResultSet.getObject();
      log.info(var);
    }

    assertDistinctResults(numberProfile, emptyProfile, filterValues, CompareFunction.EQUAL, List.of("12345"));
    assertDistinctResults(varchar2Profile, emptyProfile, filterValues, CompareFunction.EQUAL, List.of("Sample VARCHAR2"));
    assertDistinctResults(varchar2Profile, emptyProfile, multiFilterValues, CompareFunction.EQUAL, List.of("Sample VARCHAR2"));
    assertDistinctResults(varchar2Profile, varchar2Profile, varCharFilter, CompareFunction.CONTAIN, List.of("Sample VARCHAR2"));
    assertDistinctResults(varchar2Profile, emptyProfile, nonExistingFilter, CompareFunction.EQUAL, List.of());
    assertDistinctResults(varchar2Profile, emptyProfile, null, CompareFunction.EQUAL, List.of("Sample VARCHAR2"));

    // TODO implement check for LOB - distinct not supported for LOB type
    /*assertThrows(RuntimeException.class,
                 () -> assertDistinctResults(clobProfile, emptyProfile, filterValues, CompareFunction.EQUAL, List.of("B")));*/
  }

  private void assertDistinctResults(CProfile target,
                                     CProfile filterProfile,
                                     String[] filterValues,
                                     CompareFunction compareFunction,
                                     List<String> expected) throws BeginEndWrongOrderException {
    CompositeFilter compositeFilter = new CompositeFilter(
        List.of(new FilterCondition(filterProfile, filterValues, compareFunction)),
        LogicalOperator.AND);

    if (filterProfile == null || filterValues == null) {
      compositeFilter = null;
    }

    List<String> actual = dStore.getDistinct(
        tableName, target, OrderBy.ASC, compositeFilter,
        100, 0L, 1697357130000L);
    assertEquals(expected, actual);
  }

  @Test
  public void batchResultTest() {
    int fetchSize = 5;
    for (int j = 0; j < fetchSize; j++) {
      BatchResultSet rs = dStore.getBatchResultSet(tableName, 0L, 1697357130000L, j);
      while (rs.next()) {
        List<List<Object>> var = rs.getObject();
        if (j == 0) {
          assertEquals(1, var.size());
        } else if (j == 1) {
          assertEquals(1, var.size());
        } else if (j == 2) {
          assertEquals(2, var.size());
        } else if (j == 3) {
          assertEquals(3, var.size());
        } else if (j == 4) {
          assertEquals(4, var.size());
        }
        break;
      }
    }
  }

  @Test
  public void batchResultSingleTest() {
    BatchResultSet rs = dStore.getBatchResultSet(tableName, 0L, 4394908640000L, 2);
    int count = 0;
    while (rs.next()) {
      log.info(rs.getObject());
      count++;
    }
    assertEquals(2, count);
  }

  @Test
  public void batchResultSingleEmptyDataTest() {
    BatchResultSet rs = dStore.getBatchResultSet(tableName, 4394908640000L, Long.MAX_VALUE, 10);
    int count = 0;
    while (rs.next()) {
      log.info(rs.getObject());
      count++;
    }
    assertEquals(0, count);
  }

  @Test
  public void bugEmptyValueTest() throws BeginEndWrongOrderException, SqlColMetadataException {
    CProfile cProfile = tProfile.getCProfiles()
        .stream()
        .filter(f -> f.getColName().equalsIgnoreCase("mysql_dt_char_empty"))
        .findAny()
        .orElseThrow();

    List<StackedColumn> stacked =
        dStore.getStacked(tableName, cProfile, GroupFunction.COUNT, null, 0, 4394908640000L);

    assertEquals(3, stacked.get(0).getKeyCount().get(""));
    assertEquals(1, stacked.get(0).getKeyCount().get("Test"));
  }
}
