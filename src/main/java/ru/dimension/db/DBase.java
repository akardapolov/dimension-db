package ru.dimension.db;

import com.sleepycat.persist.EntityStore;
import lombok.Getter;
import org.apache.commons.dbcp2.BasicDataSource;
import ru.dimension.db.config.DBaseConfig;
import ru.dimension.db.core.BdbStore;
import ru.dimension.db.core.ChStore;
import ru.dimension.db.core.DStore;
import ru.dimension.db.core.MsSqlStore;
import ru.dimension.db.core.OracleStore;
import ru.dimension.db.core.PgSqlStore;
import ru.dimension.db.model.profile.table.BType;

public class DBase {
  private final DBaseConfig dBaseConfig;
  private final EntityStore entityStore;

  @Getter
  private final DStore dStore;

  public DBase(DBaseConfig dBaseConfig, EntityStore entityStore) {
    this.dBaseConfig = dBaseConfig;
    this.entityStore = entityStore;

    this.dStore = new BdbStore(this.dBaseConfig, this.entityStore);
  }

  public DBase(DBaseConfig dBaseConfig, BType backendType, BasicDataSource basicDataSource) {
    this.dBaseConfig = dBaseConfig;
    this.entityStore = null;

    switch (backendType) {
      case CLICKHOUSE -> this.dStore = new ChStore(this.dBaseConfig, basicDataSource);
      case POSTGRES -> this.dStore = new PgSqlStore(this.dBaseConfig, basicDataSource);
      case ORACLE -> this.dStore = new OracleStore(this.dBaseConfig, basicDataSource);
      case MSSQL -> this.dStore = new MsSqlStore(this.dBaseConfig, basicDataSource);
      default -> throw new RuntimeException("Not supported yet for: " + backendType);
    }
  }
}
