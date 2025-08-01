package ru.dimension.db.common;

import static ru.dimension.db.config.FileConfig.FILE_SEPARATOR;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.dbcp2.BasicDataSource;
import ru.dimension.db.DBase;
import ru.dimension.db.backend.BerkleyDB;
import ru.dimension.db.backend.SqlDB;
import ru.dimension.db.config.DBaseConfig;
import ru.dimension.db.core.DStore;
import ru.dimension.db.model.profile.SProfile;
import ru.dimension.db.model.profile.cstype.CSType;
import ru.dimension.db.model.profile.cstype.SType;
import ru.dimension.db.model.profile.table.AType;
import ru.dimension.db.model.profile.table.BType;
import ru.dimension.db.model.profile.table.IType;
import ru.dimension.db.model.profile.table.TType;
import ru.dimension.db.source.JdbcSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@Log4j2
@TestInstance(Lifecycle.PER_CLASS)
public abstract class AbstractClickhouseSQLTest implements JdbcSource {
  protected final String TEMP_DB_DIR = "C:\\Users\\.temp";
  protected final String BERKLEY_DB_DIR = Paths.get(TEMP_DB_DIR).toAbsolutePath().normalize() + FILE_SEPARATOR + "ch_test";
  protected BerkleyDB berkleyDB;

  protected final String dbUrl = "jdbc:clickhouse://localhost:8123";
  private final String driverClassName = "com.clickhouse.jdbc.ClickHouseDriver";

  protected Connection dbConnection;

  protected DBaseConfig dBaseConfig;
  protected DBase dBase;
  protected DStore dStore;

  private final String tableNameDataType = "ch_table_ch_dt";

  @BeforeAll
  public void initBackendAndLoad() {
    try {
      berkleyDB = new BerkleyDB(BERKLEY_DB_DIR, false);

      dBaseConfig = new DBaseConfig().setConfigDirectory(BERKLEY_DB_DIR);
      dBase = new DBase(dBaseConfig, berkleyDB.getStore());
      dStore = dBase.getDStore();

      System.getProperties().setProperty("oracle.jdbc.J2EE13Compliant", "true");

      BType bType = BType.CLICKHOUSE;
      SqlDB sqlDB = new SqlDB(bType, driverClassName, dbUrl, "admin", "admin");
      BasicDataSource basicDataSource = sqlDB.getDatasource();

      dbConnection = basicDataSource.getConnection();
    } catch (Exception e) {
      log.catching(e);
      throw new RuntimeException(e);
    }
  }

  protected SProfile getSProfileForDataTypeTest(String select) throws SQLException {
    Map<String, CSType> csTypeMap = new HashMap<>();

    getSProfileForSelect(select, dbConnection).getCsTypeMap().forEach((key, value) -> {
      if (key.equalsIgnoreCase("ch_dt_timestamp")) {
        csTypeMap.put(key, new CSType().toBuilder().isTimeStamp(true).sType(SType.RAW).build());
      } else if (key.equalsIgnoreCase("pg_dt_bytea")) {
        csTypeMap.put(key, new CSType().toBuilder().sType(SType.HISTOGRAM).build());
      } else {
        csTypeMap.put(key, new CSType().toBuilder().sType(SType.RAW).build());
      }
  });

    return new SProfile().setTableName(tableNameDataType)
            .setTableType(TType.TIME_SERIES)
            .setIndexType(IType.GLOBAL)
            .setAnalyzeType(AType.ON_LOAD)
            .setBackendType(BType.BERKLEYDB)
            .setCompression(false)
            .setCsTypeMap(csTypeMap);
  }

  @AfterAll
  public void closeDb() throws IOException {
    berkleyDB.closeDatabase();
    berkleyDB.removeDirectory();
  }
}
