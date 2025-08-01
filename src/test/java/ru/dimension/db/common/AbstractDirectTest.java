package ru.dimension.db.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.log4j.Log4j2;
import ru.dimension.db.DBase;
import ru.dimension.db.backend.BerkleyDB;
import ru.dimension.db.config.DBaseConfig;
import ru.dimension.db.core.DStore;
import ru.dimension.db.exception.BeginEndWrongOrderException;
import ru.dimension.db.exception.EnumByteExceedException;
import ru.dimension.db.exception.GanttColumnNotSupportedException;
import ru.dimension.db.exception.SqlColMetadataException;
import ru.dimension.db.exception.TableNameEmptyException;
import ru.dimension.db.metadata.DataType;
import ru.dimension.db.model.GroupFunction;
import ru.dimension.db.model.output.GanttColumnCount;
import ru.dimension.db.model.output.StackedColumn;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.SProfile;
import ru.dimension.db.model.profile.TProfile;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.io.TempDir;

@Log4j2
@TestInstance(Lifecycle.PER_CLASS)
public abstract class AbstractDirectTest {
  @TempDir
  static File databaseDir;

  protected BerkleyDB berkleyDB;

  protected DBaseConfig dBaseConfig;
  protected DBase dBase;
  protected DStore dStore;

  protected List<List<Object>> data01;
  protected List<List<Object>> data02;
  protected List<List<Object>> data03;

  private TProfile tProfile;
  protected List<CProfile> cProfiles;

  protected String tableName = "direct_table_test";

  protected String testMessage1 = "Test message 1";
  protected String testMessage2 = "Test message 2";
  protected String testMessage3 = "Test message 3";
  protected Map<String, Integer> testMap1;
  protected Map<String, Integer> testMap2;
  protected Map<String, Integer> testMap3;
  protected String[] array1;
  protected int kMap;

  protected long startTime = 1707387748310L;
  protected long longValue = 17073877482L;
  protected double doubleValue = 17073877482D;
  protected String stringValue = "a31bce67-d1ab-485b-9ffa-850385e298ac";

  private ObjectMapper objectMapper;

  @BeforeAll
  public void initBackendAndLoad() throws IOException {
    berkleyDB = new BerkleyDB(databaseDir.getAbsolutePath(), true);

    dBaseConfig = new DBaseConfig().setConfigDirectory(databaseDir.getAbsolutePath());
    dBase = new DBase(dBaseConfig, berkleyDB.getStore());
    dStore = dBase.getDStore();

    kMap = 2;
    testMap1 = new HashMap<>();
    testMap1.put("val1", 1);
    testMap1.put("val2", 2);
    testMap1.put("val3", 3);

    testMap2 = new HashMap<>();
    testMap2.put("val4", 4);
    testMap2.put("val5", 5);
    testMap2.put("val6", 6);

    testMap3 = new HashMap<>();

    array1 = new String[2];
    array1[0] = "array value 1";
    array1[1] = "array value 2";

    objectMapper = new ObjectMapper();
  }

  protected void putDataDirect(SProfile sProfile) {
    dStore = dBase.getDStore();

    try {
      tProfile = loadTableMetadata(sProfile);

      String tableName = tProfile.getTableName();
      cProfiles = tProfile.getCProfiles();

      data01 = loadData(cProfiles, 0, kMap, testMap1, testMessage1, array1);
      data02 = loadData(cProfiles, 10, kMap + 10, testMap2, testMessage2, array1);
      data03 = loadData(cProfiles, 20, kMap + 20, testMap3, testMessage3, array1);

      dStore.putDataDirect(tableName, data01);
      dStore.putDataDirect(tableName, data02);
      dStore.putDataDirect(tableName, data03);

    } catch (SqlColMetadataException | EnumByteExceedException | TableNameEmptyException e) {
      throw new RuntimeException(e);
    }
  }

  protected void putDataSimpleDirect(SProfile sProfile) {
    dStore = dBase.getDStore();

    try {
      tProfile = loadTableMetadata(sProfile);

      String tableName = tProfile.getTableName();
      cProfiles = tProfile.getCProfiles();

      AtomicInteger atomicInteger = new AtomicInteger(0);

      List<List<Object>> data = new ArrayList<>();
      data.add(atomicInteger.getAndIncrement(), addValue(startTime));
      data.add(atomicInteger.getAndIncrement(), addValue(longValue));
      data.add(atomicInteger.getAndIncrement(), addValue(doubleValue));
      data.add(atomicInteger.getAndIncrement(), addValue(stringValue));

      dStore.putDataDirect(tableName, data);

    } catch (SqlColMetadataException | EnumByteExceedException | TableNameEmptyException e) {
      throw new RuntimeException(e);
    }
  }

  protected void putDataGroupFunctionsDirect(SProfile sProfile) {
    dStore = dBase.getDStore();

    try {
      tProfile = loadTableMetadata(sProfile);

      String tableName = tProfile.getTableName();
      cProfiles = tProfile.getCProfiles();

      AtomicInteger atomicInteger1 = new AtomicInteger(0);

      List<List<Object>> data1 = new ArrayList<>();
      data1.add(atomicInteger1.getAndIncrement(), addValue(startTime));
      data1.add(atomicInteger1.getAndIncrement(), addValue(longValue));
      data1.add(atomicInteger1.getAndIncrement(), addValue(doubleValue - 1));
      data1.add(atomicInteger1.getAndIncrement(), addValue(stringValue));

      AtomicInteger atomicInteger2 = new AtomicInteger(0);
      List<List<Object>> data2 = new ArrayList<>();
      data2.add(atomicInteger2.getAndIncrement(), addValue(startTime + 1));
      data2.add(atomicInteger2.getAndIncrement(), addValue(longValue));
      data2.add(atomicInteger2.getAndIncrement(), addValue(doubleValue + 1));
      data2.add(atomicInteger2.getAndIncrement(), addValue(stringValue));

      dStore.putDataDirect(tableName, data1);
      dStore.putDataDirect(tableName, data2);

    } catch (SqlColMetadataException | EnumByteExceedException | TableNameEmptyException e) {
      throw new RuntimeException(e);
    }
  }

  private <T> ArrayList<T> addValue(T value) {
    ArrayList<T> list = new ArrayList<>(1);
    list.add(value);
    return list;
  }

  private TProfile loadTableMetadata(SProfile sProfile) throws TableNameEmptyException {
    return dStore.loadDirectTableMetadata(sProfile);
  }

  private List<List<Object>> loadData(List<CProfile> cProfiles, int start, int end, Map<String, Integer> mapData,
      String messageData, String[] array) {
    List<List<Object>> data = new ArrayList<>();
    initializeDataStructure(cProfiles, data);

    for (int i = start; i < end; i++) {
      final int index = i;
      cProfiles.forEach(v -> addToDataStructure(v, data, index, mapData, messageData, array));
    }
    return data;
  }

  private void initializeDataStructure(List<CProfile> cProfiles, List<List<Object>> data) {
    cProfiles.forEach(v -> data.add(v.getColId(), new ArrayList<>()));
  }

  private void addToDataStructure(CProfile v, List<List<Object>> data, int index, Map<String, Integer> mapData,
      String messageData, String[] array) {
    Object valueToAdd = determineValue(v, index, mapData, messageData, array);
    data.get(v.getColId()).add(valueToAdd);
  }

  private Object determineValue(CProfile profile, int index, Map<String, Integer> mapData, String messageData, String[] array) {
    DataType dType = profile.getCsType().getDType();
    if (DataType.MAP.equals(dType)) {
      return mapData;
    } else if (DataType.LONG.equals(dType)) {
      return index;
    } else if (DataType.VARCHAR.equals(dType)) {
      return messageData;
    } else if (DataType.ARRAY.equals(dType)) {
      return array;
    }
    return null;
  }

  protected List<GanttColumnCount> getDataGanttColumn(String firstColName, String secondColName, long begin, long end)
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    CProfile firstLevelGroupBy = cProfiles.stream()
        .filter(k -> k.getColName().equalsIgnoreCase(firstColName))
        .findAny().get();
    CProfile secondLevelGroupBy = cProfiles.stream()
        .filter(k -> k.getColName().equalsIgnoreCase(secondColName))
        .findAny().get();

    return getListGanttColumnTwoLevelGrouping(dStore, tProfile, firstLevelGroupBy, secondLevelGroupBy, begin, end);
  }

  public List<GanttColumnCount> getListGanttColumnTwoLevelGrouping(DStore dStore, TProfile tProfile,
                                                                   CProfile firstLevelGroupBy, CProfile secondLevelGroupBy, long begin, long end)
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    return dStore.getGantt(tProfile.getTableName(), firstLevelGroupBy, secondLevelGroupBy, begin, end);
  }

  protected void compareKeySetForMapDataType(Map<String, Integer> expectedMap, List<StackedColumn> listMapActual) {
    assertEquals(expectedMap.keySet(),
        listMapActual.stream().filter(f -> f.getKeyCount()
            .keySet().equals(expectedMap.keySet()))
            .findAny()
            .orElseThrow()
            .getKeyCount()
            .keySet());
  }

  public List<StackedColumn> getListStackedDataBySqlCol(DStore dStore, TProfile tProfile,
                                                        List<CProfile> cProfiles, String colName, GroupFunction groupFunction, long begin, long end)
      throws BeginEndWrongOrderException, SqlColMetadataException {
    return dStore.getStacked(tProfile.getTableName(), cProfiles.stream()
        .filter(k -> k.getColName().equalsIgnoreCase(colName)).findAny().orElseThrow(), groupFunction, begin, end);
  }

  public Object findListStackedKey(List<StackedColumn> list, String filter) {
    for (StackedColumn stackedColumn : list) {
      if (stackedColumn.getKeyCount().containsKey(filter)) {
        return stackedColumn.getKeyCount().entrySet()
            .stream()
            .filter((k) -> k.getKey().equals(filter)).findAny().orElseThrow().getKey();
      }
    }
    return null;
  }

  public Object findListStackedValue(List<StackedColumn> list, String filter) {
    for (StackedColumn stackedColumn : list) {
      if (stackedColumn.getKeyCount().containsKey(filter)) {
        return stackedColumn.getKeyCount().entrySet()
            .stream()
            .filter((k) -> k.getKey().equals(filter)).findAny().orElseThrow().getValue();
      }
    }
    return null;
  }

  public List<StackedColumn> getDataStackedColumn(String colName, GroupFunction groupFunction, long begin, long end)
      throws BeginEndWrongOrderException, SqlColMetadataException {
    return getListStackedDataBySqlCol(dStore, tProfile, cProfiles, colName, groupFunction, begin, end);
  }

  public List<List<Object>> getRawDataAll(long begin, long end) {
    return dStore.getRawDataAll(tProfile.getTableName(), begin, end);
  }

  protected List<GanttColumnCount> getGanttDataExpected(String fileName) throws IOException {
    return objectMapper.readValue(getStringData(fileName), new TypeReference<>() {});
  }

  protected List<StackedColumn> getStackedDataExpected(String fileName) throws IOException {
    return objectMapper.readValue(getStringData(fileName), new TypeReference<>() {});
  }

  private String getStringData(String fileName) throws IOException {
    return Files.readString(Paths.get("src", "test", "resources", "json", "direct", fileName));
  }

  @AfterAll
  public void closeDb() throws SQLException, IOException {
    berkleyDB.closeDatabase();
    berkleyDB.removeDirectory();
  }
}
