package ru.dimension.db.integration.firebird;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static ru.dimension.db.config.FileConfig.FILE_SEPARATOR;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.io.TempDir;
import ru.dimension.db.DBase;
import ru.dimension.db.backend.BerkleyDB;
import ru.dimension.db.config.DBaseConfig;
import ru.dimension.db.core.DStore;
import ru.dimension.db.exception.BeginEndWrongOrderException;
import ru.dimension.db.exception.GanttColumnNotSupportedException;
import ru.dimension.db.exception.SqlColMetadataException;
import ru.dimension.db.exception.TableNameEmptyException;
import ru.dimension.db.model.GroupFunction;
import ru.dimension.db.model.output.GanttColumnCount;
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


@Log4j2
@Disabled
@TestInstance(Lifecycle.PER_CLASS)
public class DBaseFirebirdTest implements JdbcSource {
  @TempDir
  static File tempDir;

  protected String BERKLEY_DB_DIR;
  protected BerkleyDB berkleyDB;

  protected String dbUrl;
  private final String driverClassName = "org.firebirdsql.jdbc.FBDriver";

  protected Connection dbConnection;

  protected DBaseConfig dBaseConfig;
  protected DBase dBase;
  protected DStore dStore;
  protected final String tableNameDataType = "FIREBIRD_TABLE_DATA_TYPES";

  @BeforeAll
  public void initBackendAndLoad() {
    try {
      BERKLEY_DB_DIR = Paths.get(tempDir.getAbsolutePath()).toAbsolutePath().normalize() + FILE_SEPARATOR + "firebird_test";
      dbUrl = "jdbc:firebirdsql://localhost:3050//firebird/data/testrs.fdb?charSet=UTF-8";

      berkleyDB = new BerkleyDB(BERKLEY_DB_DIR, false);

      dBaseConfig = new DBaseConfig().setConfigDirectory(BERKLEY_DB_DIR);
      dBase = new DBase(dBaseConfig, berkleyDB.getStore());
      dStore = dBase.getDStore();

      BType bType = BType.FIREBIRD;
      BasicDataSource basicDataSource = getDatasource(bType, driverClassName, dbUrl, "SYSDBA", "masterkey");

      dbConnection = basicDataSource.getConnection();
    } catch (Exception e) {
      log.catching(e);
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testDataTypes() throws Exception {
    String createTable = "CREATE TABLE " + tableNameDataType + " (" +
        "FB_DT_CLOB BLOB SUB_TYPE TEXT, " +
        "FB_DT_CHAR VARCHAR(20), " +
        "FB_DT_TEXT VARCHAR(255), " +
        "FB_DT_DATE DATE, " +
        "FB_DT_FLOAT FLOAT, " +
        // Firebird has no ENUM, use VARCHAR
        "FB_DT_ENUM VARCHAR(1), " +
        "FB_DT_INT INTEGER, " +
        "FB_DT_VARCHAR VARCHAR(100), " +
        "FB_DT_TSTZ TIMESTAMP WITH TIME ZONE)";

    String clobData = "Test clob";
    String charVal = "Sample Char";
    String textVal = "Sample TEXT";
    Date dateVal = Date.valueOf("2023-10-10");
    long dateLong = dateVal.getTime();
    float floatVal = 123.45f;
    String enumVal = "B";
    int intVal = 12345;
    String varcharVal = "Sample VARCHAR";

    try (Statement stmt = dbConnection.createStatement()) {
      try {
        stmt.executeUpdate("DROP TABLE " + tableNameDataType);
      } catch (SQLException e) {
        log.warn("Drop table failed, maybe it did not exist: {}", e.getMessage());
      }
      stmt.executeUpdate(createTable);
      stmt.executeUpdate("DELETE FROM " + tableNameDataType);
    }

    try (PreparedStatement ps = dbConnection.prepareStatement(
        "INSERT INTO " + tableNameDataType + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)")) {
      ps.setString(1, clobData);
      ps.setString(2, charVal);
      ps.setString(3, textVal);
      ps.setDate(4, dateVal);
      ps.setFloat(5, floatVal);
      ps.setString(6, enumVal);
      ps.setInt(7, intVal);
      ps.setString(8, varcharVal);
      ps.executeUpdate();
    }

    try (Statement stmt = dbConnection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableNameDataType)) {
      while (rs.next()) {
        assertEquals(clobData, rs.getString("FB_DT_CLOB"));
        assertEquals(charVal, rs.getString("FB_DT_CHAR"));
        assertEquals(textVal, rs.getString("FB_DT_TEXT"));
        assertEquals(dateVal, rs.getDate("FB_DT_DATE"));
        assertEquals(floatVal, rs.getFloat("FB_DT_FLOAT"), 0.01);
        assertEquals(enumVal, rs.getString("FB_DT_ENUM"));
        assertEquals(intVal, rs.getInt("FB_DT_INT"));
        assertEquals(varcharVal, rs.getString("FB_DT_VARCHAR"));
        assertNotNull(rs.getObject("FB_DT_TSTZ"));
      }
    }

    Map<String, CSType> csTypeMap = new HashMap<>();
    csTypeMap.put("FB_DT_DATE", new CSType().toBuilder().isTimeStamp(true).sType(SType.RAW).build());
    csTypeMap.put("FB_DT_ENUM", new CSType().toBuilder().sType(SType.ENUM).build());
    csTypeMap.put("FB_DT_FLOAT", new CSType().toBuilder().sType(SType.HISTOGRAM).build());
    csTypeMap.put("FB_DT_INT", new CSType().toBuilder().sType(SType.RAW).build());
    csTypeMap.put("FB_DT_VARCHAR", new CSType().toBuilder().sType(SType.RAW).build());
    csTypeMap.put("FB_DT_CHAR", new CSType().toBuilder().sType(SType.RAW).build());
    csTypeMap.put("FB_DT_CLOB", new CSType().toBuilder().sType(SType.RAW).build());
    csTypeMap.put("FB_DT_TEXT", new CSType().toBuilder().sType(SType.RAW).build());
    csTypeMap.put("FB_DT_TSTZ", new CSType().toBuilder().sType(SType.RAW).build());

    SProfile sProfile = new SProfile().setTableName(tableNameDataType)
        .setTableType(TType.TIME_SERIES)
        .setIndexType(IType.LOCAL)
        .setAnalyzeType(AType.ON_LOAD)
        .setBackendType(BType.BERKLEYDB)
        .setCompression(true)
        .setCsTypeMap(csTypeMap);

    TProfile tProfile = loadTableMetadata("SELECT * FROM " + tableNameDataType, sProfile);
    loadData(dStore, dbConnection, "SELECT * FROM " + tableNameDataType, sProfile, log, 20000, 20000);

    List<CProfile> cProfiles = tProfile.getCProfiles();
    String tableName = tProfile.getTableName();

    CProfile clobProfile = getCProfile(cProfiles, "FB_DT_CLOB");
    CProfile charProfile = getCProfile(cProfiles, "FB_DT_CHAR");
    CProfile textProfile = getCProfile(cProfiles, "FB_DT_TEXT");
    CProfile dateProfile = getCProfile(cProfiles, "FB_DT_DATE");
    CProfile floatProfile = getCProfile(cProfiles, "FB_DT_FLOAT");
    CProfile enumProfile = getCProfile(cProfiles, "FB_DT_ENUM");
    CProfile intProfile = getCProfile(cProfiles, "FB_DT_INT");
    CProfile varcharProfile = getCProfile(cProfiles, "FB_DT_VARCHAR");
    CProfile tstzProfile = getCProfile(cProfiles, "FB_DT_TSTZ");

    /* Test StackedColumn API */
    assertEquals(clobData, getStackedColumnKey(tableName, clobProfile));
    assertEquals(charVal, getStackedColumnKey(tableName, charProfile));
    assertEquals(textVal, getStackedColumnKey(tableName, textProfile));

    try {
      getStackedColumnKey(tableName, dateProfile);
      fail("Expected SqlColMetadataException for timestamp column");
    } catch (SqlColMetadataException e) {
      assertEquals("Not supported for timestamp column..", e.getMessage());
      log.info("Verified expected exception for timestamp column: " + e.getMessage());
    }

    assertEquals(floatVal, Float.parseFloat(getStackedColumnKey(tableName, floatProfile)), 0.01);
    assertEquals(enumVal, getStackedColumnKey(tableName, enumProfile));
    assertEquals(intVal, Integer.parseInt(getStackedColumnKey(tableName, intProfile)));
    assertEquals(varcharVal, getStackedColumnKey(tableName, varcharProfile));
    assertNotNull(getStackedColumnKey(tableName, tstzProfile));

    /* Test GanttColumn API */
    List<GanttColumnCount> clobCharGantt = getGanttColumn(tableName, clobProfile, charProfile);
    assertEquals(clobData, clobCharGantt.get(0).getKey());
    assertEquals(charVal, getGanttKey(clobCharGantt, charVal));

    List<GanttColumnCount> floatEnumGantt = getGanttColumn(tableName, floatProfile, enumProfile);
    assertEquals(floatVal, Float.parseFloat(floatEnumGantt.get(0).getKey()), 0.01);
    assertEquals(enumVal, getGanttKey(floatEnumGantt, enumVal));

    /* Test Raw data API */
    dStore.getRawDataAll(tableName, 0, Long.MAX_VALUE).forEach(row -> cProfiles.forEach(cProfile -> {
      try {
        if (cProfile.equals(dateProfile)) {
          long actualLong = (long) dStore.getRawDataByColumn(tableName, dateProfile, 0, Long.MAX_VALUE).getLast().getLast();
          assertEquals(dateLong, actualLong);
        }
      } catch (Exception e) {
        log.info(e.getMessage());
        throw new RuntimeException(e);
      }
    }));
  }

  private BasicDataSource getDatasource(BType bType,
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

      if (BType.FIREBIRD.equals(bType)) {
        basicDataSource.setValidationQuery("SELECT 1 FROM RDB$DATABASE");
      }

      basicDataSource.setInitialSize(3);
      basicDataSource.setMaxTotal(7);
      basicDataSource.setMaxWaitMillis(TimeUnit.SECONDS.toMillis(5));

    } catch (Exception e) {
      log.catching(e);
    }
    return basicDataSource;
  }

  @AfterAll
  public void closeDb() throws IOException, SQLException {
    if (dbConnection != null && !dbConnection.isClosed()) {
      dbConnection.close();
    }
    berkleyDB.closeDatabase();
    berkleyDB.removeDirectory();
  }

  private TProfile loadTableMetadata(String query, SProfile sProfile)
      throws SQLException, TableNameEmptyException {
    return dStore.loadJdbcTableMetadata(dbConnection, query, sProfile);
  }

  private CProfile getCProfile(List<CProfile> cProfiles, String colName) {
    return cProfiles.stream()
        .filter(c -> c.getColName().equalsIgnoreCase(colName))
        .findFirst()
        .orElseThrow();
  }

  private String getStackedColumnKey(String tableName, CProfile cProfile)
      throws BeginEndWrongOrderException, SqlColMetadataException {
    return dStore.getStacked(tableName, cProfile, GroupFunction.COUNT, null, 0, Long.MAX_VALUE)
        .get(0)
        .getKeyCount()
        .keySet()
        .iterator()
        .next();
  }

  private String getGanttKey(List<GanttColumnCount> ganttColumnCountList, String filter) {
    return ganttColumnCountList.get(0).getGantt()
        .entrySet()
        .stream()
        .filter(f -> f.getKey().equalsIgnoreCase(filter))
        .findAny()
        .orElseThrow()
        .getKey();
  }

  private List<GanttColumnCount> getGanttColumn(String tableName, CProfile cProfileFirst, CProfile cProfileSecond)
      throws BeginEndWrongOrderException, SqlColMetadataException, GanttColumnNotSupportedException {
    return dStore.getGanttCount(tableName, cProfileFirst, cProfileSecond, null, 0, Long.MAX_VALUE);
  }
}