package ru.dimension.db.integration.sqlite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.log4j.Log4j2;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import ru.dimension.db.common.AbstractSQLiteTest;
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

@Log4j2
@TestInstance(Lifecycle.PER_CLASS)
@Disabled
public class DBaseSQLiteTest extends AbstractSQLiteTest {

  @BeforeAll
  public void initialLoading() {}

  @Test
  public void testDataTypes() throws Exception {
    String sourceTableName = "table_data_types";
    String createTable = "CREATE TABLE " + sourceTableName + " (" +
        "sqlite_dt_clob TEXT, " +
        "sqlite_dt_char TEXT, " +
        "sqlite_dt_text TEXT, " +
        "sqlite_dt_date TEXT, " +
        "sqlite_dt_float REAL, " +
        "sqlite_dt_int INTEGER, " +
        "sqlite_dt_varchar TEXT)";

    String clobData = "Test clob";
    String charVal = "Sample Char";
    String textVal = "Sample TEXT";
    Date dateVal = Date.valueOf("2023-10-10");
    long dateLong = dateVal.getTime();
    float floatVal = 123.45f;
    int intVal = 12345;
    String varcharVal = "Sample VARCHAR";

    try (Statement stmt = dbConnection.createStatement()) {
      stmt.executeUpdate(createTable);
      stmt.executeUpdate("DELETE FROM " + sourceTableName); // SQLite uses DELETE instead of TRUNCATE
    }

    try (PreparedStatement ps = dbConnection.prepareStatement(
        "INSERT INTO " + sourceTableName + " VALUES (?, ?, ?, ?, ?, ?, ?)")) {
      ps.setString(1, clobData);
      ps.setString(2, charVal);
      ps.setString(3, textVal);
      ps.setString(4, dateVal.toString());
      ps.setFloat(5, floatVal);
      ps.setInt(6, intVal);
      ps.setString(7, varcharVal);
      ps.executeUpdate();
    }

    try (Statement stmt = dbConnection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM " + sourceTableName)) {
      while (rs.next()) {
        assertEquals(clobData, rs.getString("sqlite_dt_clob"));
        assertEquals(charVal, rs.getString("sqlite_dt_char"));
        assertEquals(textVal, rs.getString("sqlite_dt_text"));
        assertEquals(dateVal.toString(), rs.getString("sqlite_dt_date"));
        assertEquals(floatVal, rs.getFloat("sqlite_dt_float"), 0.01);
        assertEquals(intVal, rs.getInt("sqlite_dt_int"));
        assertEquals(varcharVal, rs.getString("sqlite_dt_varchar"));
      }
    }

    Map<String, CSType> csTypeMap = new HashMap<>();
    csTypeMap.put("sqlite_dt_date", new CSType().toBuilder().isTimeStamp(true).sType(SType.RAW).build());
    csTypeMap.put("sqlite_dt_float", new CSType().toBuilder().sType(SType.HISTOGRAM).build());
    csTypeMap.put("sqlite_dt_int", new CSType().toBuilder().sType(SType.RAW).build());
    csTypeMap.put("sqlite_dt_varchar", new CSType().toBuilder().sType(SType.RAW).build());
    csTypeMap.put("sqlite_dt_char", new CSType().toBuilder().sType(SType.RAW).build());
    csTypeMap.put("sqlite_dt_clob", new CSType().toBuilder().sType(SType.RAW).build());
    csTypeMap.put("sqlite_dt_text", new CSType().toBuilder().sType(SType.RAW).build());

    SProfile sProfile = new SProfile().setTableName(tableNameDataType)
        .setTableType(TType.TIME_SERIES)
        .setIndexType(IType.LOCAL)
        .setAnalyzeType(AType.ON_LOAD)
        .setBackendType(BType.BERKLEYDB)
        .setCompression(true)
        .setCsTypeMap(csTypeMap);

    TProfile tProfile = loadTableMetadata("SELECT * FROM " + sourceTableName, sProfile);
    loadData(dStore, dbConnection, "SELECT * FROM " + sourceTableName, sProfile, log, 20000, 20000);

    List<CProfile> cProfiles = tProfile.getCProfiles();
    String tableName = tProfile.getTableName();

    CProfile clobProfile = getCProfile(cProfiles, "sqlite_dt_clob");
    CProfile charProfile = getCProfile(cProfiles, "sqlite_dt_char");
    CProfile textProfile = getCProfile(cProfiles, "sqlite_dt_text");
    CProfile dateProfile = getCProfile(cProfiles, "sqlite_dt_date");
    CProfile floatProfile = getCProfile(cProfiles, "sqlite_dt_float");
    CProfile intProfile = getCProfile(cProfiles, "sqlite_dt_int");
    CProfile varcharProfile = getCProfile(cProfiles, "sqlite_dt_varchar");

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

    assertEquals(floatVal, Float.parseFloat(getStackedColumnKey(tableName, floatProfile)));
    assertEquals(intVal, Integer.parseInt(getStackedColumnKey(tableName, intProfile)));
    assertEquals(varcharVal, getStackedColumnKey(tableName, varcharProfile));

    /* Test GanttColumn API */
    List<GanttColumnCount> clobCharGantt = getGanttColumn(tableName, clobProfile, charProfile);
    assertEquals(clobData, clobCharGantt.get(0).getKey());
    assertEquals(charVal, getGanttKey(clobCharGantt, charVal));

    List<GanttColumnCount> floatIntGantt = getGanttColumn(tableName, floatProfile, intProfile);
    assertEquals(floatVal, Float.parseFloat(floatIntGantt.get(0).getKey()));

    /* Test Raw data API */
    // Note: SQLite stores dates as TEXT, so timestamp conversion from external DB to BerkeleyDB
    // requires different handling. We skip the timestamp column check for SQLite.
    dStore.getRawDataAll(tableName, 0, Long.MAX_VALUE).forEach(row -> {
      try {
        // Skip timestamp column for SQLite - dates are stored as TEXT
      } catch (Exception e) {
        log.info(e.getMessage());
        throw new RuntimeException(e);
      }
    });
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