package ru.dimension.db.integration.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
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
import ru.dimension.db.model.output.GanttColumnCount;
import ru.dimension.db.model.output.GanttColumnSum;
import ru.dimension.db.model.output.StackedColumn;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.SProfile;
import ru.dimension.db.model.profile.TProfile;
import ru.dimension.db.model.profile.table.BType;
import ru.dimension.db.model.profile.table.TType;
import ru.dimension.db.sql.BatchResultSet;

@Log4j2
@TestInstance(Lifecycle.PER_CLASS)
@Disabled
public class DBaseOracleRegularBackendTest extends AbstractBackendSQLTest {

  protected final String dbUrl = "jdbc:oracle:thin:@localhost:1523:orcl";
  private final String driverClassName = "oracle.jdbc.driver.OracleDriver";
  private final String tableName = "ORACLE_REGULAR_DATA";
  private final String select = "select * from " + tableName + " where rownum < 2";

  String createTable = """
      CREATE TABLE oracle_regular_data (
          id NUMBER GENERATED ALWAYS AS IDENTITY,
          name VARCHAR2(100),
          category VARCHAR2(50),
          amount NUMBER(10, 2),
          description VARCHAR2(200),
          status VARCHAR2(20)
      )
      """;

  private SProfile sProfile;
  private TProfile tProfile;

  @BeforeAll
  public void setUp() throws SQLException, TableNameEmptyException {
    BType bType = BType.ORACLE;
    BasicDataSource basicDataSource = getDatasource(bType, driverClassName, dbUrl, "system", "sys");

    if (tableExists(basicDataSource.getConnection(), tableName)) {
      dropTableOracle(basicDataSource.getConnection(), tableName);
    }

    try (Statement createTableStmt = basicDataSource.getConnection().createStatement()) {
      createTableStmt.executeUpdate(createTable);
    }

    String insertQuery = """
        INSERT INTO oracle_regular_data (name, category, amount, description, status)
        VALUES (?, ?, ?, ?, ?)
        """;

    String[][] data = {
        {"Alice", "Electronics", "150.00", "Laptop purchase", "ACTIVE"},
        {"Bob", "Electronics", "200.00", "Phone purchase", "ACTIVE"},
        {"Charlie", "Books", "30.00", "Novel", "INACTIVE"},
        {"Diana", "Books", "45.00", "Textbook", "ACTIVE"},
        {"Eve", "Clothing", "80.00", "Jacket", "INACTIVE"},
        {"Frank", "Electronics", "350.00", "Tablet purchase", "ACTIVE"},
        {"Grace", "Books", "25.00", "Magazine", "ACTIVE"},
        {"Hank", "Clothing", "120.00", "Coat", "INACTIVE"},
        {"Ivy", "Electronics", "500.00", "Monitor", "ACTIVE"},
        {"Jack", "Clothing", "60.00", "Shirt", "ACTIVE"},
    };

    try (PreparedStatement ps = basicDataSource.getConnection().prepareStatement(insertQuery)) {
      for (String[] row : data) {
        ps.setString(1, row[0]);
        ps.setString(2, row[1]);
        ps.setBigDecimal(3, new java.math.BigDecimal(row[2]));
        ps.setString(4, row[3]);
        ps.setString(5, row[4]);
        ps.executeUpdate();
      }
    }

    initMetaDataBackend(bType, basicDataSource);

    sProfile = getSProfileForRegularBackend(tableName, basicDataSource, bType, select);
    tProfile = dStore.loadJdbcTableMetadata(basicDataSource.getConnection(), select, sProfile);

    assertEquals(TType.REGULAR, tProfile.getTableType());

    log.info(tProfile);
  }

  @Test
  public void getDistinctRegularTest() throws BeginEndWrongOrderException {
    CProfile cProfile = getCProfileByName(tProfile, "CATEGORY");

    List<String> listActual = dStore.getDistinct(tableName, cProfile, OrderBy.ASC, null, 100, 0L, Long.MAX_VALUE);

    assertEquals(3, listActual.size());
    assertTrue(listActual.contains("Electronics"));
    assertTrue(listActual.contains("Books"));
    assertTrue(listActual.contains("Clothing"));
  }

  @Test
  public void getDistinctRegularWithFilterTest() throws BeginEndWrongOrderException {
    CProfile categoryProfile = getCProfileByName(tProfile, "CATEGORY");
    CProfile statusProfile = getCProfileByName(tProfile, "STATUS");

    String[] filterValues = {"ACTIVE"};
    CompositeFilter compositeFilter = new CompositeFilter(
        List.of(new FilterCondition(statusProfile, filterValues, CompareFunction.EQUAL)),
        LogicalOperator.AND);

    List<String> actualResults = dStore.getDistinct(
        tableName, categoryProfile, OrderBy.ASC, compositeFilter, 100, 0L, Long.MAX_VALUE);

    assertTrue(actualResults.contains("Electronics"));
  }

  @Test
  public void getStackedRegularCountTest() throws SqlColMetadataException, BeginEndWrongOrderException {
    CProfile categoryProfile = getCProfileByName(tProfile, "CATEGORY");

    List<StackedColumn> stackedColumns = dStore.getStacked(
        tableName, categoryProfile, GroupFunction.COUNT, null, 0, Long.MAX_VALUE);

    assertNotNull(stackedColumns);
    assertFalse(stackedColumns.isEmpty());

    StackedColumn column = stackedColumns.get(0);
    assertEquals(4, column.getKeyCount().get("Electronics"));
    assertEquals(3, column.getKeyCount().get("Books"));
    assertEquals(3, column.getKeyCount().get("Clothing"));
  }

  @Test
  public void getGanttCountRegularTest() throws Exception {
    CProfile categoryProfile = getCProfileByName(tProfile, "CATEGORY");
    CProfile statusProfile = getCProfileByName(tProfile, "STATUS");

    List<GanttColumnCount> ganttColumns = dStore.getGanttCount(
        tableName, categoryProfile, statusProfile, null, 0, Long.MAX_VALUE);

    assertNotNull(ganttColumns);
    assertFalse(ganttColumns.isEmpty());
  }

  @Test
  public void getGanttSumRegularTest() throws Exception {
    CProfile categoryProfile = getCProfileByName(tProfile, "CATEGORY");
    CProfile amountProfile = getCProfileByName(tProfile, "AMOUNT");

    List<GanttColumnSum> ganttSums = dStore.getGanttSum(
        tableName, categoryProfile, amountProfile, null, 0, Long.MAX_VALUE);

    assertNotNull(ganttSums);
    assertFalse(ganttSums.isEmpty());
  }

  @Test
  public void batchResultRegularTest() {
    BatchResultSet batchResultSet = dStore.getBatchResultSet(tableName, 3);

    int totalRows = 0;
    while (batchResultSet.next()) {
      List<List<Object>> batch = batchResultSet.getObject();
      assertNotNull(batch);
      assertFalse(batch.isEmpty());
      totalRows += batch.size();
    }

    assertEquals(10, totalRows);
  }

  @Test
  public void batchResultRegularSmallFetchTest() {
    BatchResultSet batchResultSet = dStore.getBatchResultSet(tableName, 2);

    int batchCount = 0;
    int totalRows = 0;
    while (batchResultSet.next()) {
      List<List<Object>> batch = batchResultSet.getObject();
      assertNotNull(batch);
      totalRows += batch.size();
      batchCount++;
    }

    assertEquals(10, totalRows);
    assertEquals(5, batchCount);
  }

  protected static boolean tableExists(Connection connection,
                                       String tableName) throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    try (var resultSet = metaData.getTables(null, null, tableName, null)) {
      return resultSet.next();
    }
  }
}