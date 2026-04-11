package ru.dimension.db.common;

import static ru.dimension.db.config.FileConfig.FILE_SEPARATOR;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
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
public abstract class AbstractSQLiteTest implements JdbcSource {

  @TempDir
  static File tempDir;

  protected String BERKLEY_DB_DIR;
  protected BerkleyDB berkleyDB;

  protected String dbUrl;
  private final String driverClassName = "org.sqlite.JDBC";

  protected Connection dbConnection;
  protected BasicDataSource basicDataSource;

  protected DBaseConfig dBaseConfig;
  protected DBase dBase;
  protected DStore dStore;
  protected final String tableNameDataType = "table_data_types";

  @BeforeAll
  public void initBackendAndLoad() {
    try {
      BERKLEY_DB_DIR = Paths.get(tempDir.getAbsolutePath())
          .toAbsolutePath()
          .normalize()
          + FILE_SEPARATOR + "sqlite_test";

      dbUrl = "jdbc:sqlite:" + tempDir.getAbsolutePath() + FILE_SEPARATOR + "test.db";

      berkleyDB = new BerkleyDB(BERKLEY_DB_DIR, false);

      dBaseConfig = new DBaseConfig().setConfigDirectory(BERKLEY_DB_DIR);
      dBase = new DBase(dBaseConfig, berkleyDB.getStore());
      dStore = dBase.getDStore();

      BType bType = BType.SQLITE;
      basicDataSource = getDatasource(bType, driverClassName, dbUrl, null, null);
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
    BasicDataSource ds = null;
    try {
      ds = new BasicDataSource();
      ds.setDriverClassName(driverClassName);
      ds.setUrl(dbUrl);
      ds.setValidationQueryTimeout(5);

      if (username != null) {
        ds.setUsername(username);
        ds.setPassword(password);
      }

      if (BType.SQLITE.equals(bType)) {
        ds.setValidationQuery("SELECT 1");
        ds.setInitialSize(1);
        ds.setMaxTotal(1);
      } else if (BType.CLICKHOUSE.equals(bType)) {
        ds.setValidationQuery("SELECT 1");
        ds.addConnectionProperty("compress", "0");
        ds.setInitialSize(3);
        ds.setMaxTotal(7);
      } else {
        ds.setInitialSize(3);
        ds.setMaxTotal(7);
      }

      ds.setMaxWaitMillis(TimeUnit.SECONDS.toMillis(5));

    } catch (Exception e) {
      log.catching(e);
    }

    return ds;
  }

  @AfterAll
  public void closeDb() throws IOException, SQLException {
    if (dbConnection != null && !dbConnection.isClosed()) {
      dbConnection.close();
      log.info("dbConnection closed");
    }

    if (basicDataSource != null && !basicDataSource.isClosed()) {
      basicDataSource.close();
      log.info("BasicDataSource closed");
    }

    berkleyDB.closeDatabase();
    berkleyDB.removeDirectory();
  }
}