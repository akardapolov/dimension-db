package ru.dimension.db.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.dbcp2.BasicDataSource;
import ru.dimension.db.DBase;
import ru.dimension.db.config.DBaseConfig;
import ru.dimension.db.core.DStore;
import ru.dimension.db.model.output.StackedColumn;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.SProfile;
import ru.dimension.db.model.profile.TProfile;
import ru.dimension.db.model.profile.cstype.CSType;
import ru.dimension.db.model.profile.cstype.SType;
import ru.dimension.db.model.profile.table.AType;
import ru.dimension.db.model.profile.table.BType;
import ru.dimension.db.model.profile.table.IType;
import ru.dimension.db.model.profile.table.TType;
import ru.dimension.db.source.JdbcSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.io.TempDir;

@Log4j2
@TestInstance(Lifecycle.PER_CLASS)
public abstract class AbstractBackendSQLTest implements JdbcSource {

  @TempDir
  static File databaseDir;

  protected DBaseConfig dBaseConfig;
  protected DBase dBase;
  protected DStore dStore;

  private ObjectMapper objectMapper;

  public void initMetaDataBackend(BType bType,
                                  BasicDataSource basicDataSource) {
    try {
      System.getProperties().setProperty("oracle.jdbc.J2EE13Compliant", "true");

      dBaseConfig = new DBaseConfig().setConfigDirectory(databaseDir.getAbsolutePath());
      dBase = new DBase(dBaseConfig, bType, basicDataSource);
      dStore = dBase.getDStore();

      objectMapper = new ObjectMapper();
    } catch (Exception e) {
      log.catching(e);
      throw new RuntimeException(e);
    }
  }

  protected SProfile getSProfileForBackend(String tableName,
                                           BasicDataSource basicDataSource,
                                           BType bType,
                                           String select,
                                           String tsName) throws SQLException {
    Map<String, CSType> csTypeMap = new HashMap<>();

    getSProfileForSelect(select, basicDataSource.getConnection()).getCsTypeMap().forEach((key, value) -> {
      if (key.equalsIgnoreCase(tsName)) {
        csTypeMap.put(key, new CSType().toBuilder().isTimeStamp(true).sType(SType.RAW).build());
      } else {
        csTypeMap.put(key, new CSType().toBuilder().isTimeStamp(false).sType(SType.RAW).build());
      }
    });

    return new SProfile().setTableName(tableName)
        .setTableType(TType.TIME_SERIES)
        .setIndexType(IType.GLOBAL)
        .setAnalyzeType(AType.ON_LOAD)
        .setBackendType(bType)
        .setCompression(false)
        .setCsTypeMap(csTypeMap);
  }

  protected BasicDataSource getDatasource(BType bType,
                                          String driverClassName,
                                          String dbUrl,
                                          String username,
                                          String password) {
    BasicDataSource basicDataSource = null;
    try {
      basicDataSource = new BasicDataSource();

      basicDataSource.setDriverClassName(driverClassName);
      basicDataSource.setUrl(dbUrl);

      basicDataSource.setValidationQueryTimeout(5);

      if (username != null) {
        basicDataSource.setUsername(username);
        basicDataSource.setPassword(password);
      }

      if (BType.CLICKHOUSE.equals(bType)) {
        basicDataSource.setValidationQuery("SELECT 1");
        basicDataSource.addConnectionProperty("compress", "0");
      }

      basicDataSource.setInitialSize(3);
      basicDataSource.setMaxTotal(7);
      basicDataSource.setMaxWaitMillis(TimeUnit.SECONDS.toMillis(5));

    } catch (Exception e) {
      log.catching(e);
    }

    return basicDataSource;
  }

  protected long getUnixTimestamp(LocalDateTime localDateTime) {
    ZoneOffset offset = ZoneId.systemDefault().getRules().getOffset(localDateTime);
    return localDateTime.toInstant(offset).toEpochMilli();
  }

  protected static void dropTable(Connection connection,
                                  String tableName) throws SQLException {
    String sql = "DROP TABLE IF EXISTS " + tableName;

    try (Statement statement = connection.createStatement()) {
      log.info("SQL statement: " + sql);
      statement.executeUpdate(sql);
      log.info("Table dropped successfully!");
    }
  }

  protected static void dropTableOracle(Connection connection, String tableName) throws SQLException {
    String sql = "DROP TABLE " + tableName;

    try (Statement statement = connection.createStatement()) {
      statement.executeUpdate(sql);
      log.info("Table dropped successfully!");
    }
  }

  protected void dropTableMySQL(java.sql.Connection con, String tableName) throws SQLException {
    try (java.sql.Statement st = con.createStatement()) {
      st.executeUpdate("DROP TABLE IF EXISTS " + tableName);
    }
  }

  protected static void dropTableMSSQL(Connection connection, String tableName) throws SQLException {
    String sql = "DROP TABLE " + tableName;

    try (Statement statement = connection.createStatement()) {
      statement.executeUpdate(sql);
      log.info("Table dropped successfully!");
    } catch (SQLException e) {
      // Handle the case where the table does not exist
      if (e.getErrorCode() == 3701) { // SQL Server error code for "Cannot drop the table because it does not exist."
        log.info("Skip drop operation, table does not exist in DB.");
      } else {
        throw e;
      }
    }
  }

  protected static boolean tableExists(Connection connection, String tableName) throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();

    try (var resultSet = metaData.getTables(null, null, tableName, null)) {
      return resultSet.next();
    }
  }

  public void assertStackedListEquals(List<StackedColumn> expected,
                                      List<StackedColumn> actual) {
    assertEquals(expected.stream().findAny().orElseThrow().getKeyCount(),
                 actual.stream().findAny().orElseThrow().getKeyCount());
  }

  protected List<StackedColumn> getStackedDataExpected(String fileName) throws IOException {
    return objectMapper.readValue(getStackedTestData(fileName), new TypeReference<>() {
    });
  }

  protected CProfile getCProfileByName(TProfile tProfile,
                                     String colName) {
    return tProfile.getCProfiles().stream()
        .filter(f -> f.getColName().equalsIgnoreCase(colName))
        .findAny().orElseThrow();
  }

  private String getStackedTestData(String fileName) throws IOException {
    return Files.readString(Paths.get("src", "test", "resources", "json", "stacked", fileName));
  }

  @AfterAll
  public void closeDb() throws IOException {
  }
}