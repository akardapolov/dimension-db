package ru.dimension.db.model;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.Accessors;
import ru.dimension.db.DBase;
import ru.dimension.db.backend.BerkleyDB;
import ru.dimension.db.config.DBaseConfig;
import ru.dimension.db.core.DStore;
import ru.dimension.db.source.H2Database;

@AllArgsConstructor
@Accessors(chain = true)
@Data
@Builder(toBuilder = true)
public class DBaseTestConfig {
  private final DBaseConfig dBaseConfig;
  private final BerkleyDB berkleyDB;

  private final DBase dBase;
  private final DStore dStore;

  private final H2Database h2Db;
  private final Connection h2DbConnection;

  public DBaseTestConfig(File databaseDir, H2Database h2DbIn) throws IOException {
    // DBase initialization
    dBaseConfig = new DBaseConfig().setConfigDirectory(databaseDir.getAbsolutePath());
    berkleyDB = new BerkleyDB(databaseDir.getAbsolutePath(), true);
    dBase = new DBase(dBaseConfig, berkleyDB.getStore());
    dStore = dBase.getDStore();

    // Set H2Database and JDBC Connection
    h2Db = h2DbIn;
    h2DbConnection = h2DbIn.getConnection();
  }
}
