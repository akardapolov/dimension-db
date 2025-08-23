package ru.dimension.db.integration.mysql;

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
import ru.dimension.db.common.AbstractMySQLTest;
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
@Disabled
public class DBaseMySQLTest extends AbstractMySQLTest {

  @BeforeAll
  public void initialLoading() {}

  @Test
  public void testDataTypes() throws Exception {
    String createTable = "CREATE TABLE IF NOT EXISTS mysql_data_types (" +
        "mysql_dt_clob TEXT, " +
        "mysql_dt_char CHAR(24), " +
        "mysql_dt_text TEXT, " +
        "mysql_dt_date DATE, " +
        "mysql_dt_float FLOAT, " +
        "mysql_dt_enum ENUM('A','B','C'), " +
        "mysql_dt_int INT, " +
        "mysql_dt_varchar VARCHAR(32))";

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
      stmt.executeUpdate(createTable);
      stmt.executeUpdate("TRUNCATE TABLE mysql_data_types");
    }

    try (PreparedStatement ps = dbConnection.prepareStatement(
        "INSERT INTO mysql_data_types VALUES (?, ?, ?, ?, ?, ?, ?, ?)")) {
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
        ResultSet rs = stmt.executeQuery("SELECT * FROM mysql_data_types")) {
      while (rs.next()) {
        assertEquals(clobData, rs.getString("mysql_dt_clob"));
        assertEquals(charVal, rs.getString("mysql_dt_char").trim());
        assertEquals(textVal, rs.getString("mysql_dt_text"));
        assertEquals(dateVal, rs.getDate("mysql_dt_date"));
        assertEquals(floatVal, rs.getFloat("mysql_dt_float"), 0.01);
        assertEquals(enumVal, rs.getString("mysql_dt_enum"));
        assertEquals(intVal, rs.getInt("mysql_dt_int"));
        assertEquals(varcharVal, rs.getString("mysql_dt_varchar"));
      }
    }

    Map<String, CSType> csTypeMap = new HashMap<>();
    csTypeMap.put("mysql_dt_date", new CSType().toBuilder().isTimeStamp(true).sType(SType.RAW).build());
    csTypeMap.put("mysql_dt_enum", new CSType().toBuilder().sType(SType.ENUM).build());
    csTypeMap.put("mysql_dt_float", new CSType().toBuilder().sType(SType.HISTOGRAM).build());
    csTypeMap.put("mysql_dt_int", new CSType().toBuilder().sType(SType.RAW).build());
    csTypeMap.put("mysql_dt_varchar", new CSType().toBuilder().sType(SType.RAW).build());
    csTypeMap.put("mysql_dt_char", new CSType().toBuilder().sType(SType.RAW).build());
    csTypeMap.put("mysql_dt_clob", new CSType().toBuilder().sType(SType.RAW).build());
    csTypeMap.put("mysql_dt_text", new CSType().toBuilder().sType(SType.RAW).build());

    SProfile sProfile = new SProfile().setTableName(tableNameDataType)
        .setTableType(TType.TIME_SERIES)
        .setIndexType(IType.LOCAL)
        .setAnalyzeType(AType.ON_LOAD)
        .setBackendType(BType.BERKLEYDB)
        .setCompression(true)
        .setCsTypeMap(csTypeMap);

    TProfile tProfile = loadTableMetadata("SELECT * FROM mysql_data_types", sProfile);
    loadData(dStore, dbConnection, "SELECT * FROM mysql_data_types", sProfile, log, 20000, 20000);

    List<CProfile> cProfiles = tProfile.getCProfiles();
    String tableName = tProfile.getTableName();

    CProfile clobProfile = getCProfile(cProfiles, "mysql_dt_clob");
    CProfile charProfile = getCProfile(cProfiles, "mysql_dt_char");
    CProfile textProfile = getCProfile(cProfiles, "mysql_dt_text");
    CProfile dateProfile = getCProfile(cProfiles, "mysql_dt_date");
    CProfile floatProfile = getCProfile(cProfiles, "mysql_dt_float");
    CProfile enumProfile = getCProfile(cProfiles, "mysql_dt_enum");
    CProfile intProfile = getCProfile(cProfiles, "mysql_dt_int");
    CProfile varcharProfile = getCProfile(cProfiles, "mysql_dt_varchar");

    /* Test StackedColumn API */
    assertEquals(clobData, getStackedColumnKey(tableName, clobProfile));
    assertEquals(charVal, getStackedColumnKey(tableName, charProfile));
    assertEquals(textVal, getStackedColumnKey(tableName, textProfile));

    try {
      String dateKey = getStackedColumnKey(tableName, dateProfile);
      fail("Expected SqlColMetadataException for timestamp column");
    } catch (SqlColMetadataException e) {
      assertEquals("Not supported for timestamp column..", e.getMessage());
      log.info("Verified expected exception for timestamp column: " + e.getMessage());
    }

    assertEquals(floatVal, Float.valueOf(getStackedColumnKey(tableName, floatProfile)));
    assertEquals(enumVal, getStackedColumnKey(tableName, enumProfile));
    assertEquals(intVal, Integer.valueOf(getStackedColumnKey(tableName, intProfile)));
    assertEquals(varcharVal, getStackedColumnKey(tableName, varcharProfile));

    /* Test GanttColumn API */
    List<GanttColumnCount> clobCharGantt = getGanttColumn(tableName, clobProfile, charProfile);
    assertEquals(clobData, clobCharGantt.get(0).getKey());
    assertEquals(charVal, getGanttKey(clobCharGantt, charVal));

    List<GanttColumnCount> charTextGantt = getGanttColumn(tableName, charProfile, textProfile);
    assertEquals(charVal, charTextGantt.get(0).getKey());
    assertEquals(textVal, getGanttKey(charTextGantt, textVal));

    List<GanttColumnCount> floatEnumGantt = getGanttColumn(tableName, floatProfile, enumProfile);
    assertEquals(floatVal, Float.valueOf(floatEnumGantt.get(0).getKey()));
    assertEquals(enumVal, getGanttKey(floatEnumGantt, enumVal));

    List<GanttColumnCount> enumIntGantt = getGanttColumn(tableName, enumProfile, intProfile);
    assertEquals(enumVal, enumIntGantt.get(0).getKey());
    assertEquals(intVal, Integer.valueOf(getGanttKey(enumIntGantt, Integer.toString(intVal))));

    List<GanttColumnCount> intVarcharGantt = getGanttColumn(tableName, intProfile, varcharProfile);
    assertEquals(intVal, Integer.valueOf(intVarcharGantt.get(0).getKey()));
    assertEquals(varcharVal, getGanttKey(intVarcharGantt, varcharVal));

    /* Test Raw data API */
    List<List<Object>> rawDataAll = dStore.getRawDataAll(tableName, 0, Long.MAX_VALUE);
    rawDataAll.forEach(row -> cProfiles.forEach(cProfile -> {
      try {
        if (cProfile.equals(clobProfile)) assertEquals(clobData, getStackedColumnKey(tableName, clobProfile));
        if (cProfile.equals(charProfile)) assertEquals(charVal, getStackedColumnKey(tableName, charProfile));
        if (cProfile.equals(textProfile)) assertEquals(textVal, getStackedColumnKey(tableName, textProfile));

        if (cProfile.equals(dateProfile)) {
          long actualLong = (long) dStore.getRawDataByColumn(tableName, dateProfile, 0, Long.MAX_VALUE).getLast().getLast();
          assertEquals(dateLong, actualLong);
        }

        if (cProfile.equals(floatProfile)) assertEquals(floatVal, Float.valueOf(getStackedColumnKey(tableName, floatProfile)));
        if (cProfile.equals(enumProfile)) assertEquals(enumVal, getStackedColumnKey(tableName, enumProfile));
        if (cProfile.equals(intProfile)) assertEquals(intVal, Integer.valueOf(getStackedColumnKey(tableName, intProfile)));
        if (cProfile.equals(varcharProfile)) assertEquals(varcharVal, getStackedColumnKey(tableName, varcharProfile));
      } catch (Exception e) {
        log.info(e.getMessage());
        throw new RuntimeException(e);
      }
    }));
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