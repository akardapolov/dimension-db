package ru.dimension.db.integration.ch;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.dbcp2.BasicDataSource;
import ru.dimension.db.common.AbstractBackendSQLTest;
import ru.dimension.db.exception.BeginEndWrongOrderException;
import ru.dimension.db.exception.TableNameEmptyException;
import ru.dimension.db.model.CompareFunction;
import ru.dimension.db.model.OrderBy;
import ru.dimension.db.model.filter.CompositeFilter;
import ru.dimension.db.model.filter.FilterCondition;
import ru.dimension.db.model.filter.LogicalOperator;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.SProfile;
import ru.dimension.db.model.profile.TProfile;
import ru.dimension.db.model.profile.table.BType;
import ru.dimension.db.sql.BatchResultSet;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@Log4j2
@TestInstance(Lifecycle.PER_CLASS)
@Disabled
public class DBaseClickHouseSQLRSBackendTest extends AbstractBackendSQLTest {

  private final String dbUrl = "jdbc:clickhouse://localhost:8124";
  private final String driverClassName = "com.clickhouse.jdbc.ClickHouseDriver";
  private final String tableName = "default.ch_data_rs";
  private final String tsName = "CH_DT_TIMESTAMP";
  private final String select = "select * from " + tableName + " limit 1";

  String createTableRs = """
           CREATE TABLE default.ch_data_rs (
                  ch_dt_dec Decimal(10, 2),
                  ch_dt_int Int32,
                  ch_dt_byte Int8,
                  ch_dt_bool Boolean,
                  ch_dt_char FixedString(1),
                  ch_dt_clob String,
                  ch_dt_date Date,
                  ch_dt_timestamp DateTime('Europe/Moscow')
                ) ENGINE = MergeTree() ORDER BY (ch_dt_int)
      """;

  private SProfile sProfile;
  private TProfile tProfile;

  LocalDateTime ch_dt_timestamp = LocalDateTime.of(2023, 10, 13, 16, 5, 20);

  @BeforeAll
  public void setUp() throws SQLException, TableNameEmptyException {
    BType bType = BType.CLICKHOUSE;
    BasicDataSource basicDataSource = getDatasource(bType, driverClassName, dbUrl, "admin", "admin");

    // Prepare remote backend
    dropTable(basicDataSource.getConnection(), tableName);

    try (Statement createTableStmt = basicDataSource.getConnection().createStatement()) {
      createTableStmt.executeUpdate(createTableRs);
    }

    BigDecimal ch_dt_dec = new BigDecimal("1234.56");
    int ch_dt_int = 6789;
    byte ch_dt_byte = 12;
    boolean ch_dt_bool = true;
    String ch_dt_char = "A";
    String ch_dt_clob = "Lorem ipsum dolor sit amet";
    LocalDate ch_dt_date = LocalDate.of(2023, 10, 13);

    String insertQuery = """
        INSERT INTO default.ch_data_rs
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """;

    LocalDateTime[] timestamps = new LocalDateTime[]{
        ch_dt_timestamp,
        ch_dt_timestamp.plusDays(1),
        ch_dt_timestamp.plusDays(2)
    };

    try (PreparedStatement ps = basicDataSource.getConnection().prepareStatement(insertQuery)) {
      for (LocalDateTime timestamp : timestamps) {
        ps.setBigDecimal(1, ch_dt_dec);
        ps.setInt(2, ch_dt_int);
        ps.setByte(3, ch_dt_byte);
        ps.setBoolean(4, ch_dt_bool);
        ps.setString(5, ch_dt_char);
        ps.setString(6, ch_dt_clob);
        ps.setDate(7, java.sql.Date.valueOf(ch_dt_date));
        ps.setTimestamp(8, java.sql.Timestamp.valueOf(timestamp));

        ps.executeUpdate();
      }
    } catch (SQLException e) {
      log.error(e);
      throw new RuntimeException(e);
    }

    initMetaDataBackend(bType, basicDataSource);

    sProfile = getSProfileForBackend(tableName, basicDataSource, bType, select, tsName);
    tProfile = dStore.loadJdbcTableMetadata(basicDataSource.getConnection(), select, sProfile);

    log.info(tProfile);
  }

  @Test
  public void getDistinctTest() throws BeginEndWrongOrderException {
    CProfile cProfile = getCProfileByName(tProfile, "CH_DT_CHAR");

    List<String> listActual = dStore.getDistinct(tableName, cProfile, OrderBy.DESC, null, 100, 0L, 4394908640000L);

    assertEquals(List.of("A"), listActual);
  }

  @Test
  public void getDistinctTestWithFilter() throws BeginEndWrongOrderException {
    CProfile intProfile = getCProfileByName(tProfile, "CH_DT_INT");
    CProfile charProfile = getCProfileByName(tProfile, "CH_DT_CHAR");
    CProfile clobProfile = getCProfileByName(tProfile, "CH_DT_CLOB");
    CProfile boolProfile = getCProfileByName(tProfile, "CH_DT_BOOL");

    String[] filterValues = {"A"};
    String[] clobFilter = {"Lorem"};
    String[] nonExistingFilter = {"NonExisting"};

    assertDistinctResults(intProfile, charProfile, filterValues, CompareFunction.EQUAL, List.of("6789"));

    assertDistinctResults(charProfile, intProfile, new String[]{"6789"}, CompareFunction.EQUAL, List.of("A"));

    assertDistinctResults(charProfile, charProfile, filterValues, CompareFunction.EQUAL, List.of("A"));

    assertDistinctResults(clobProfile, clobProfile, clobFilter, CompareFunction.CONTAIN, List.of("Lorem ipsum dolor sit amet"));

    assertDistinctResults(charProfile, charProfile, nonExistingFilter, CompareFunction.EQUAL, List.of());

    assertDistinctResults(charProfile, charProfile, null, CompareFunction.EQUAL, List.of("A"));

    assertDistinctResults(boolProfile, charProfile, filterValues, CompareFunction.EQUAL, List.of("true"));
  }

  private void assertDistinctResults(CProfile targetProfile, CProfile filterProfile,
                                     String[] filterValues, CompareFunction compareFunction,
                                     List<String> expectedResults) throws BeginEndWrongOrderException {
    CompositeFilter compositeFilter = new CompositeFilter(
        List.of(new FilterCondition(filterProfile,
                                    filterValues,
                                    compareFunction)),
        LogicalOperator.AND);

    if (filterProfile == null || filterValues == null) {
      compositeFilter = null;
    }

    List<String> actualResults = dStore.getDistinct(
        tableName,
        targetProfile,
        OrderBy.ASC,
        compositeFilter,
        100,
        0L,
        4394908640000L);
    assertEquals(expectedResults, actualResults);
  }

  @Test
  public void batchResultTest() {
    int fetchSize = 4;

    for (int j = 0; j < fetchSize; j++) {
      BatchResultSet batchResultSet = dStore.getBatchResultSet(tableName, 0L, 1697357130000L, j);

      while (batchResultSet.next()) {
        List<List<Object>> var = batchResultSet.getObject();

        if (j == 0) {
          assertEquals(1, var.size());
        } else if (j == 1) {
          assertEquals(1, var.size());
        } else if (j == 2) {
          assertEquals(2, var.size());
        } else if (j == 3) {
          assertEquals(3, var.size());
        } else if (j == 4) {
            assertEquals(3, var.size());
        }

        break;
      }
    }
  }

  @Test
  public void batchResultSingleTest() {
    BatchResultSet batchResultSet = dStore.getBatchResultSet(tableName, 0L, 4394908640000L, 10);

    while (batchResultSet.next()) {
      log.info(batchResultSet.getObject());
    }
  }

  @Test
  public void batchResultSingleEmptyDataTest() {
    BatchResultSet batchResultSet = dStore.getBatchResultSet(tableName, 4394908640000L, Long.MAX_VALUE, 10);

    int count = 0;
    while (batchResultSet.next()) {
      log.info(batchResultSet.getObject());

      count++;
    }

    assertEquals(0, count);
  }
}
