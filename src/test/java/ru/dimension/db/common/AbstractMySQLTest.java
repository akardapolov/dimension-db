package ru.dimension.db.common;

import static ru.dimension.db.config.FileConfig.FILE_SEPARATOR;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.concurrent.TimeUnit;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.dbcp2.BasicDataSource;
import ru.dimension.db.DBase;
import ru.dimension.db.backend.BerkleyDB;
import ru.dimension.db.config.DBaseConfig;
import ru.dimension.db.core.DStore;
import ru.dimension.db.model.profile.table.BType;
import ru.dimension.db.source.JdbcSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@Log4j2
@TestInstance(Lifecycle.PER_CLASS)
public abstract class AbstractMySQLTest implements JdbcSource {
  protected final String TEMP_DB_DIR = "C:\\Users\\.temp";
  protected final String BERKLEY_DB_DIR = Paths.get(TEMP_DB_DIR).toAbsolutePath().normalize() + FILE_SEPARATOR + "mysql_test";
  protected BerkleyDB berkleyDB;

  protected final String dbUrl = "jdbc:mysql://localhost:3307/dimension-db?createDatabaseIfNotExist=true";
  private final String driverClassName = "com.mysql.cj.jdbc.Driver";

  protected Connection dbConnection;

  protected DBaseConfig dBaseConfig;
  protected DBase dBase;
  protected DStore dStore;
  protected final String tableNameDataType = "mysql_table_data_types";

  @BeforeAll
  public void initBackendAndLoad() {
    try {
      berkleyDB = new BerkleyDB(BERKLEY_DB_DIR, false);

      dBaseConfig = new DBaseConfig().setConfigDirectory(BERKLEY_DB_DIR);
      dBase = new DBase(dBaseConfig, berkleyDB.getStore());
      dStore = dBase.getDStore();

      BType bType = BType.MYSQL;
      BasicDataSource basicDataSource = getDatasource(bType, driverClassName, dbUrl, "root", "root");

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

  @AfterAll
  public void closeDb() throws IOException {
    berkleyDB.closeDatabase();
    berkleyDB.removeDirectory();
  }
}