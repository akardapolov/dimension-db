package ru.dimension.db.integration.ch;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import lombok.extern.log4j.Log4j2;
import ru.dimension.db.DBase;
import ru.dimension.db.backend.BerkleyDB;
import ru.dimension.db.config.DBaseConfig;
import ru.dimension.db.core.DStore;
import ru.dimension.db.exception.TableNameEmptyException;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.TProfile;
import ru.dimension.db.source.ClickHouse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@Log4j2
@TestInstance(Lifecycle.PER_CLASS)
@Disabled
public class DBaseCHQueryRawTest implements ClickHouse {

  private DStore dStore;
  private TProfile tProfile;
  private List<CProfile> cProfiles;

  private BerkleyDB berkleyDB;

  private ObjectMapper objectMapper;

  @BeforeAll
  public void initialLoading() throws IOException {
    String dbFolder = getTestDbFolder();

    this.berkleyDB = new BerkleyDB(dbFolder, false);

    DBaseConfig dBaseConfig = new DBaseConfig().setConfigDirectory(dbFolder);
    DBase dBase = new DBase(dBaseConfig, berkleyDB.getStore());
    dStore = dBase.getDStore();

    try {
      tProfile = dBase.getDStore().getTProfile(tableName);
      log.info(tProfile);
    } catch (TableNameEmptyException e) {
      throw new RuntimeException(e);
    }
    cProfiles = tProfile.getCProfiles();

    objectMapper = new ObjectMapper();
  }

  @Test
  public void stackedColumnTripTypeCountGroupByTest() {
    String query = """
        SELECT *
        FROM datasets.trips_mergetree
        WHERE trip_id = 1189193759
        AND pickup_datetime = toDateTime('2016-01-17 19:27:37');
        """;
    log.info("Query raw data by query: " + "\n" + query);

    CProfile cProfile = getCProfileByName("TRIP_ID");

    String filterValueExpected = "1189193759";

    long begin = getUnixTimestamp(LocalDateTime.of(2016, 1, 17, 19, 27, 37, 0));
    long end = getUnixTimestamp(LocalDateTime.of(2016, 1, 17, 19, 27, 37, 999999999));

    List<List<Object>> actualRaw = dStore.getRawDataAll(tProfile.getTableName(), begin, end);

    List<List<Object>> actualRawFilter =
        dStore.getRawDataAll(tProfile.getTableName(), cProfile, filterValueExpected, begin, end);

    log.info("Raw data all: " + "\n" + actualRaw);
    log.info("Raw data filtered by: " + "\n" + actualRawFilter);

    assertFilterValueInData(actualRaw, filterValueExpected);
    assertFilterValueInData(actualRawFilter, filterValueExpected);
  }

  private CProfile getCProfileByName(String colName) {
    return tProfile.getCProfiles().stream()
        .filter(f -> f.getColName().equals(colName))
        .findAny().orElseThrow();
  }

  protected long getUnixTimestamp(LocalDateTime localDateTime) {
    ZoneOffset offset = ZoneId.systemDefault().getRules().getOffset(localDateTime);
    return localDateTime.toInstant(offset).toEpochMilli();
  }

  private void assertFilterValueInData(List<List<Object>> data, String expectedValue) {
    String actualValue = data.stream()
        .flatMap(Collection::stream)
        .filter(expectedValue::equals)
        .findAny()
        .orElseThrow(() -> new AssertionError("Expected value not found"))
        .toString();
    assertEquals(expectedValue, actualValue);
  }

  @AfterAll
  public void closeDb() {
    berkleyDB.closeDatabase();
  }
}
