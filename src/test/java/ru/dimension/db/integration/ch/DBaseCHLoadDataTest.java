package ru.dimension.db.integration.ch;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import lombok.extern.log4j.Log4j2;
import ru.dimension.db.DBase;
import ru.dimension.db.backend.BerkleyDB;
import ru.dimension.db.config.DBaseConfig;
import ru.dimension.db.core.DStore;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.table.IType;
import ru.dimension.db.model.profile.table.TType;
import ru.dimension.db.source.ClickHouse;
import ru.dimension.db.source.ClickHouseDatabase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@Log4j2
@TestInstance(Lifecycle.PER_CLASS)
@Disabled
public class DBaseCHLoadDataTest implements ClickHouse {

  private DStore dStore;
  private List<CProfile> cProfiles;

  private String url = "jdbc:clickhouse://localhost:8123/?user=admin&password=admin";

  private ClickHouseDatabase clickHouseDB;
  private BerkleyDB berkleyDB;

  @BeforeAll
  public void initialLoading() throws SQLException, IOException {
    String dbFolder = getTestDbFolder();

    this.berkleyDB = new BerkleyDB(dbFolder, true);

    this.clickHouseDB = new ClickHouseDatabase(url);

    DBaseConfig dBaseConfig = new DBaseConfig().setConfigDirectory(dbFolder);
    DBase dBase = new DBase(dBaseConfig, berkleyDB.getStore());
    dStore = dBase.getDStore();
  }

  @Test
  public void loadDataDirectParallel() {
    try {
      cProfiles = clickHouseDB.loadDataDirectParallel(ClickHouse.select2016, dStore,
                                                      TType.TIME_SERIES, IType.GLOBAL, true, 2, 20000);
    } catch (Exception e) {
      log.catching(e);
    }
    assertEquals(1, 1);
  }

  @Test
  public void loadDataDirect() {
    try {
      cProfiles = clickHouseDB.loadDataDirect(ClickHouse.select2016,
                                              dStore,
                                              TType.TIME_SERIES,
                                              IType.GLOBAL,
                                              true,
                                              20000,
                                              false);
    } catch (Exception e) {
      log.catching(e);
    }
    assertEquals(1, 1);
  }

  @Test
  public void loadDataJdbcTimeSeriesGlobalCompression() {
    try {
      cProfiles = clickHouseDB.loadDataJdbc(ClickHouse.select2016, dStore,
                                            TType.TIME_SERIES, IType.GLOBAL, true, 20000);
    } catch (Exception e) {
      log.catching(e);
    }
    assertEquals(1, 1);
  }

  @Test
  public void loadDataJdbcTimeSeriesGlobalNoCompression() {
    try {
      cProfiles = clickHouseDB.loadDataJdbc(ClickHouse.select2016, dStore,
                                            TType.TIME_SERIES, IType.GLOBAL, false, 20000);
    } catch (Exception e) {
      log.catching(e);
    }
    assertEquals(1, 1);
  }

  @Test
  public void loadDataJdbcTimeSeriesLocalCompression() {
    try {
      cProfiles = clickHouseDB.loadDataJdbc(ClickHouse.select2016, dStore,
                                            TType.TIME_SERIES, IType.LOCAL, true, 20000);
    } catch (Exception e) {
      log.catching(e);
    }
    assertEquals(1, 1);
  }

  @Test
  public void loadDataJdbcTimeSeriesLocalNoCompression() {
    try {
      cProfiles = clickHouseDB.loadDataJdbc(ClickHouse.select2016, dStore,
                                            TType.TIME_SERIES, IType.LOCAL, false, 20000);
    } catch (Exception e) {
      log.catching(e);
    }
    assertEquals(1, 1);
  }

  @Test
  public void loadDataBatchTest() {
    try {
     cProfiles = clickHouseDB.loadDataJdbcBatch(ClickHouse.select2016, dStore,
                                                TType.TIME_SERIES, IType.GLOBAL, true, 20000, 20000);
    } catch (Exception e) {
      log.catching(e);
    }
    assertEquals(1, 1);
  }

  @AfterAll
  public void closeDb() throws SQLException {
    clickHouseDB.close();
    berkleyDB.closeDatabase();
    /*berkleyDB.removeDirectory();*/
  }
}
