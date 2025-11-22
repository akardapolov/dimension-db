package ru.dimension.db.common;

import static ru.dimension.db.config.FileConfig.FILE_SEPARATOR;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.concurrent.TimeUnit;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.io.TempDir;
import ru.dimension.db.DBase;
import ru.dimension.db.backend.BerkleyDB;
import ru.dimension.db.config.DBaseConfig;
import ru.dimension.db.core.DStore;
import ru.dimension.db.model.profile.table.BType;
import ru.dimension.db.source.JdbcSource;

@Log4j2
@TestInstance(Lifecycle.PER_CLASS)
public abstract class AbstractDuckDBTest implements JdbcSource {
  @TempDir
  static File tempDir;

  protected String BERKLEY_DB_DIR;
  protected BerkleyDB berkleyDB;

  protected String dbUrl;
  private final String driverClassName = "org.duckdb.DuckDBDriver";

  protected Connection dbConnection;

  protected DBaseConfig dBaseConfig;
  protected DBase dBase;
  protected DStore dStore;
  protected final String tableNameDataType = "duckdb_table_data_types";

  @BeforeAll
  public void initBackendAndLoad() {
    try {
      BERKLEY_DB_DIR = Paths.get(tempDir.getAbsolutePath()).toAbsolutePath().normalize() + FILE_SEPARATOR + "duckdb_test";

      // TODO File based DuckDB - The process cannot access the file because it is being used by another process
      //dbUrl = "jdbc:duckdb:" + Paths.get(tempDir.getAbsolutePath()).toAbsolutePath().normalize() + FILE_SEPARATOR + "duck.db";

      // Memory based DuckDB
      dbUrl = "jdbc:duckdb::memory:testdb";

      berkleyDB = new BerkleyDB(BERKLEY_DB_DIR, false);

      dBaseConfig = new DBaseConfig().setConfigDirectory(BERKLEY_DB_DIR);
      dBase = new DBase(dBaseConfig, berkleyDB.getStore());
      dStore = dBase.getDStore();

      BType bType = BType.DUCKDB;
      BasicDataSource basicDataSource = getDatasource(bType, driverClassName, dbUrl, null, null); // DuckDB file doesn't need user/pass

      dbConnection = basicDataSource.getConnection();
    } catch (Exception e) {
      log.catching(e);
      throw new RuntimeException(e);
    }
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

      if (BType.DUCKDB.equals(bType)) {
        basicDataSource.setValidationQuery("SELECT 1");
      } else if (BType.CLICKHOUSE.equals(bType)) {
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

  @AfterAll
  public void closeDb() throws IOException {
    berkleyDB.closeDatabase();
    berkleyDB.removeDirectory();
  }
}