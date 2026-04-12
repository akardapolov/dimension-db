package ru.dimension.db.integration.pqsql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
public class DBasePgSQLRegularBackendTest extends AbstractBackendSQLTest {

  private final String dbUrl = "jdbc:postgresql://localhost:5432/postgres";
  private final String driverClassName = "org.postgresql.Driver";
  private final String tableName = "pg_regular_data";
  private final String select = "select * from " + tableName + " limit 1";

  String createTable = """
      CREATE TABLE pg_regular_data (
          id serial,
          name varchar(100),
          category varchar(50),
          amount numeric(10, 2),
          description text,
          status varchar(20)
      )
      """;

  private SProfile sProfile;
  private TProfile tProfile;

  @BeforeAll
  public void setUp() throws SQLException, TableNameEmptyException {
    BType bType = BType.POSTGRES;
    BasicDataSource basicDataSource = getDatasource(bType, driverClassName, dbUrl, "postgres", "postgres");

    dropTable(basicDataSource.getConnection(), tableName);

    try (Statement createTableStmt = basicDataSource.getConnection().createStatement()) {
      createTableStmt.executeUpdate(createTable);
    }

    String insertQuery = """
        INSERT INTO pg_regular_data (name, category, amount, description, status)
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
    assertTrue(actualResults.contains("Books"));
    assertTrue(actualResults.contains("Clothing"));
  }

  @Test
  public void getDistinctRegularContainFilterTest() throws BeginEndWrongOrderException {
    CProfile nameProfile = getCProfileByName(tProfile, "NAME");
    CProfile descProfile = getCProfileByName(tProfile, "DESCRIPTION");

    String[] filterValues = {"purchase"};
    CompositeFilter compositeFilter = new CompositeFilter(
        List.of(new FilterCondition(descProfile, filterValues, CompareFunction.CONTAIN)),
        LogicalOperator.AND);

    List<String> actualResults = dStore.getDistinct(
        tableName, nameProfile, OrderBy.ASC, compositeFilter, 100, 0L, Long.MAX_VALUE);

    assertTrue(actualResults.contains("Alice"));
    assertTrue(actualResults.contains("Bob"));
    assertTrue(actualResults.contains("Frank"));
    assertFalse(actualResults.contains("Charlie"));
  }

  @Test
  public void getDistinctRegularWithLimitTest() throws BeginEndWrongOrderException {
    CProfile nameProfile = getCProfileByName(tProfile, "NAME");

    List<String> actualResults = dStore.getDistinct(
        tableName, nameProfile, OrderBy.ASC, null, 3, 0L, Long.MAX_VALUE);

    assertEquals(3, actualResults.size());
  }

  @Test
  public void getStackedRegularCountTest() throws SqlColMetadataException, BeginEndWrongOrderException {
    CProfile categoryProfile = getCProfileByName(tProfile, "CATEGORY");

    List<StackedColumn> stackedColumns = dStore.getStacked(
        tableName, categoryProfile, GroupFunction.COUNT, null, 0, Long.MAX_VALUE);

    assertNotNull(stackedColumns);
    assertFalse(stackedColumns.isEmpty());

    StackedColumn column = stackedColumns.getFirst();
    assertEquals(4, column.getKeyCount().get("Electronics"));
    assertEquals(3, column.getKeyCount().get("Books"));
    assertEquals(3, column.getKeyCount().get("Clothing"));
  }

  @Test
  public void getStackedRegularCountWithFilterTest() throws SqlColMetadataException, BeginEndWrongOrderException {
    CProfile categoryProfile = getCProfileByName(tProfile, "CATEGORY");
    CProfile statusProfile = getCProfileByName(tProfile, "STATUS");

    String[] filterValues = {"ACTIVE"};
    CompositeFilter compositeFilter = new CompositeFilter(
        List.of(new FilterCondition(statusProfile, filterValues, CompareFunction.EQUAL)),
        LogicalOperator.AND);

    List<StackedColumn> stackedColumns = dStore.getStacked(
        tableName, categoryProfile, GroupFunction.COUNT, compositeFilter, 0, Long.MAX_VALUE);

    assertNotNull(stackedColumns);
    assertFalse(stackedColumns.isEmpty());

    StackedColumn column = stackedColumns.getFirst();
    assertEquals(4, column.getKeyCount().get("Electronics"));
    assertEquals(2, column.getKeyCount().get("Books"));
    assertEquals(1, column.getKeyCount().get("Clothing"));
  }

  @Test
  public void getStackedRegularSumTest() throws SqlColMetadataException, BeginEndWrongOrderException {
    CProfile amountProfile = getCProfileByName(tProfile, "AMOUNT");

    List<StackedColumn> stackedColumns = dStore.getStacked(
        tableName, amountProfile, GroupFunction.SUM, null, 0, Long.MAX_VALUE);

    assertNotNull(stackedColumns);
    assertFalse(stackedColumns.isEmpty());
  }

  @Test
  public void getStackedRegularAvgTest() throws SqlColMetadataException, BeginEndWrongOrderException {
    CProfile amountProfile = getCProfileByName(tProfile, "AMOUNT");

    List<StackedColumn> stackedColumns = dStore.getStacked(
        tableName, amountProfile, GroupFunction.AVG, null, 0, Long.MAX_VALUE);

    assertNotNull(stackedColumns);
    assertFalse(stackedColumns.isEmpty());
  }

  @Test
  public void getGanttCountRegularTest() throws Exception {
    CProfile categoryProfile = getCProfileByName(tProfile, "CATEGORY");
    CProfile statusProfile = getCProfileByName(tProfile, "STATUS");

    List<GanttColumnCount> ganttColumns = dStore.getGanttCount(
        tableName, categoryProfile, statusProfile, null, 0, Long.MAX_VALUE);

    assertNotNull(ganttColumns);
    assertFalse(ganttColumns.isEmpty());

    boolean foundElectronics = ganttColumns.stream()
        .anyMatch(g -> g.getKey().equals("Electronics"));
    assertTrue(foundElectronics);
  }

  @Test
  public void getGanttCountRegularWithFilterTest() throws Exception {
    CProfile categoryProfile = getCProfileByName(tProfile, "CATEGORY");
    CProfile statusProfile = getCProfileByName(tProfile, "STATUS");
    CProfile nameProfile = getCProfileByName(tProfile, "NAME");

    String[] filterValues = {"ACTIVE"};
    CompositeFilter compositeFilter = new CompositeFilter(
        List.of(new FilterCondition(statusProfile, filterValues, CompareFunction.EQUAL)),
        LogicalOperator.AND);

    List<GanttColumnCount> ganttColumns = dStore.getGanttCount(
        tableName, categoryProfile, nameProfile, compositeFilter, 0, Long.MAX_VALUE);

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

    boolean foundElectronics = ganttSums.stream()
        .anyMatch(g -> g.getKey().equals("Electronics"));
    assertTrue(foundElectronics);
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

  @Test
  public void batchResultRegularLargeFetchTest() {
    BatchResultSet batchResultSet = dStore.getBatchResultSet(tableName, 100);

    int batchCount = 0;
    int totalRows = 0;
    while (batchResultSet.next()) {
      List<List<Object>> batch = batchResultSet.getObject();
      assertNotNull(batch);
      totalRows += batch.size();
      batchCount++;
    }

    assertEquals(10, totalRows);
    assertEquals(1, batchCount);
  }

  @Test
  public void batchResultRegularColumnCountTest() {
    BatchResultSet batchResultSet = dStore.getBatchResultSet(tableName, 5);

    while (batchResultSet.next()) {
      List<List<Object>> batch = batchResultSet.getObject();

      for (List<Object> row : batch) {
        assertEquals(6, row.size());
      }
      break;
    }
  }

  @Test
  public void multipleFiltersAndTest() throws BeginEndWrongOrderException {
    CProfile nameProfile = getCProfileByName(tProfile, "NAME");
    CProfile categoryProfile = getCProfileByName(tProfile, "CATEGORY");
    CProfile statusProfile = getCProfileByName(tProfile, "STATUS");

    CompositeFilter compositeFilter = new CompositeFilter(
        List.of(
            new FilterCondition(categoryProfile, new String[]{"Electronics"}, CompareFunction.EQUAL),
            new FilterCondition(statusProfile, new String[]{"ACTIVE"}, CompareFunction.EQUAL)
        ),
        LogicalOperator.AND);

    List<String> actualResults = dStore.getDistinct(
        tableName, nameProfile, OrderBy.ASC, compositeFilter, 100, 0L, Long.MAX_VALUE);

    assertTrue(actualResults.contains("Alice"));
    assertTrue(actualResults.contains("Bob"));
    assertTrue(actualResults.contains("Frank"));
    assertTrue(actualResults.contains("Ivy"));
    assertFalse(actualResults.contains("Charlie"));
    assertFalse(actualResults.contains("Eve"));
  }

  @Test
  public void multipleFiltersOrTest() throws BeginEndWrongOrderException {
    CProfile nameProfile = getCProfileByName(tProfile, "NAME");
    CProfile categoryProfile = getCProfileByName(tProfile, "CATEGORY");

    CompositeFilter compositeFilter = new CompositeFilter(
        List.of(
            new FilterCondition(categoryProfile, new String[]{"Electronics"}, CompareFunction.EQUAL),
            new FilterCondition(categoryProfile, new String[]{"Books"}, CompareFunction.EQUAL)
        ),
        LogicalOperator.OR);

    List<String> actualResults = dStore.getDistinct(
        tableName, nameProfile, OrderBy.ASC, compositeFilter, 100, 0L, Long.MAX_VALUE);

    assertTrue(actualResults.contains("Alice"));
    assertTrue(actualResults.contains("Charlie"));
    assertFalse(actualResults.contains("Eve"));
    assertFalse(actualResults.contains("Hank"));
  }

  @Test
  public void emptyFilterTest() throws BeginEndWrongOrderException {
    CProfile categoryProfile = getCProfileByName(tProfile, "CATEGORY");

    List<String> withNull = dStore.getDistinct(
        tableName, categoryProfile, OrderBy.ASC, null, 100, 0L, Long.MAX_VALUE);

    List<String> withEmpty = dStore.getDistinct(
        tableName, categoryProfile, OrderBy.ASC,
        new CompositeFilter(List.of(), LogicalOperator.AND), 100, 0L, Long.MAX_VALUE);

    assertEquals(withNull, withEmpty);
  }

  @Test
  public void batchResultRegularWithBeginEndTest() {
    BatchResultSet batchResultSet = dStore.getBatchResultSet(tableName, 0L, Long.MAX_VALUE, 5);

    int totalRows = 0;
    while (batchResultSet.next()) {
      List<List<Object>> batch = batchResultSet.getObject();
      assertNotNull(batch);
      totalRows += batch.size();
    }

    assertEquals(10, totalRows);
  }

  protected static void dropTable(java.sql.Connection connection,
                                  String tableName) throws SQLException {
    String sql = "DROP TABLE IF EXISTS " + tableName;
    try (Statement statement = connection.createStatement()) {
      statement.executeUpdate(sql);
    }
  }
}