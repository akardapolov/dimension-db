package ru.dimension.db.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
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
import ru.dimension.db.model.BdbAndDBase;
import ru.dimension.db.model.BeginAndEnd;
import ru.dimension.db.model.CompareFunction;
import ru.dimension.db.model.FSColAndRange;
import ru.dimension.db.model.FSColumn;
import ru.dimension.db.model.FilterColumn;
import ru.dimension.db.model.GroupFunction;
import ru.dimension.db.model.LoadDataType;
import ru.dimension.db.model.PermutationState;
import ru.dimension.db.model.Person;
import ru.dimension.db.model.filter.CompositeFilter;
import ru.dimension.db.model.filter.FilterCondition;
import ru.dimension.db.model.filter.LogicalOperator;
import ru.dimension.db.model.output.GanttColumnCount;
import ru.dimension.db.model.output.StackedColumn;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.SProfile;
import ru.dimension.db.model.profile.TProfile;
import ru.dimension.db.model.profile.cstype.CSType;
import ru.dimension.db.model.profile.cstype.CType;
import ru.dimension.db.model.profile.cstype.SType;
import ru.dimension.db.model.profile.table.AType;
import ru.dimension.db.model.profile.table.BType;
import ru.dimension.db.model.profile.table.IType;
import ru.dimension.db.model.profile.table.TType;
import ru.dimension.db.service.mapping.Mapper;
import ru.dimension.db.source.H2Database;
import ru.dimension.db.source.JdbcSource;
import ru.dimension.db.sql.BatchResultSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.io.TempDir;

@Log4j2
@TestInstance(Lifecycle.PER_CLASS)
public abstract class AbstractH2Test implements JdbcSource {
  @TempDir
  static File databaseDir;

  protected BerkleyDB berkleyDB;

  protected H2Database h2Db;

  protected Connection dbConnection;

  protected DBaseConfig dBaseConfig;
  protected DBase dBase;
  protected DStore dStore;

  protected List<List<Object>> data01;
  protected List<List<Object>> data02;
  protected List<List<Object>> data03;
  protected List<List<Object>> data04;
  protected List<List<Object>> data05;
  protected List<List<Object>> data06;
  protected List<List<Object>> data07;
  protected List<List<Object>> data08;

  protected LocalDateTime birthday = LocalDateTime.of(2023, 1, 1, 1, 1, 1);

  protected TProfile tProfile;
  private List<CProfile> cProfilesH2Db;
  protected String select = "SELECT * FROM person WHERE ROWNUM < 2";

  protected String tableName = "h2_table_test";

  protected static ExecutorService executorService;

  protected static Boolean[] boolArray = {Boolean.TRUE, Boolean.FALSE};

  @BeforeAll
  public void initBackendAndLoad() throws SQLException, IOException {
    h2Db = new H2Database("jdbc:h2:mem:test");
    berkleyDB = new BerkleyDB(databaseDir.getAbsolutePath(), true);

    dBaseConfig = new DBaseConfig().setConfigDirectory(databaseDir.getAbsolutePath());
    dBase = new DBase(dBaseConfig, berkleyDB.getStore());
    dStore = dBase.getDStore();

    dbConnection = h2Db.getConnection();

    h2Db.execute( """
          CREATE TABLE PERSON (
            id        INT,
            firstname VARCHAR(64),
            lastname  VARCHAR(64),
            house     INT,
            city      VARCHAR(64),
            birthday  TIMESTAMP
          )
        """);

    h2Db.insert(Person.builder().id(1).firstname("Alex").lastname("Ivanov").house(1).city("Moscow").birthday(birthday).build());
    h2Db.insert(Person.builder().id(2).firstname("Ivan").lastname("Ivanov").house(2).city("Moscow").birthday(birthday).build());
    h2Db.insert(Person.builder().id(3).firstname("Oleg").lastname("Petrov").house(1).city("Moscow").birthday(birthday).build());
    h2Db.insert(Person.builder().id(4).firstname("Lee").lastname("Sui").house(1).city("Moscow").birthday(birthday).build());
    h2Db.insert(Person.builder().id(5).firstname("Lee").lastname("Ivanov").house(1).city("Moscow").birthday(birthday).build());
    h2Db.insert(Person.builder().id(6).firstname("Lee").lastname("Ivanov").house(2).city("Moscow").birthday(birthday).build());

    h2Db.loadSqlColMetadataList("SELECT * FROM person");
    data01 = h2Db.getData("SELECT * FROM person");

    h2Db.insert(Person.builder().id(7).firstname("Men").lastname("Petrov").house(1).city("Yekaterinburg").birthday(birthday).build());
    data02 = h2Db.getData("SELECT * FROM person WHERE id=7");

    h2Db.insert(Person.builder().id(8).firstname("Ion").lastname("Тихий").house(1).city("Moscow").birthday(birthday).build());
    h2Db.insert(Person.builder().id(9).firstname("Федор").lastname("Шаляпин").house(1).city("Moscow").birthday(birthday).build());
    h2Db.insert(Person.builder().id(10).firstname("Петр").lastname("Пирогов").house(1).city("Moscow").birthday(birthday).build());
    data03 = h2Db.getData("SELECT * FROM person WHERE id=10 OR id=8 OR id=9");

    h2Db.insert(Person.builder().id(11).firstname("Oleg").lastname("Semenov").house(1).city("Moscow").birthday(birthday).build());
    data04 = h2Db.getData("SELECT * FROM person WHERE id=11");

    h2Db.insert(Person.builder().id(12).firstname("Oleg").lastname("Mirko").house(2).city("Yekaterinburg").birthday(birthday).build());
    h2Db.insert(Person.builder().id(13).firstname("Oleg").lastname("Vedel").house(3).city("Moscow").birthday(birthday).build());
    h2Db.insert(Person.builder().id(14).firstname("Oleg").lastname("Tan").house(1).city("Moscow").birthday(birthday).build());
    data05 = h2Db.getData("SELECT * FROM person WHERE id=12 OR id=13 OR id=14");

    h2Db.insert(Person.builder().id(15).firstname("Egor").lastname("Semenov").house(1).city("Yekaterinburg").birthday(birthday).build());
    h2Db.insert(Person.builder().id(16).firstname("Egor").lastname("Semenov").house(1).city("Yekaterinburg").birthday(birthday).build());
    h2Db.insert(Person.builder().id(17).firstname("Egor").lastname("Semenov").house(1).city("Moscow").birthday(birthday).build());
    h2Db.insert(Person.builder().id(18).firstname("Egor").lastname("Semenov").house(2).city("Moscow").birthday(birthday).build());
    h2Db.insert(Person.builder().id(19).firstname("Egor").lastname("Semenov").house(2).city("Yekaterinburg").birthday(birthday).build());
    h2Db.insert(Person.builder().id(20).firstname("Egor").lastname("Semenov").house(2).city("Yekaterinburg").birthday(birthday).build());
    h2Db.insert(Person.builder().id(21).firstname("Egor").lastname("Semenov").house(3).city("Yekaterinburg").birthday(birthday).build());
    h2Db.insert(Person.builder().id(22).firstname("Egor").lastname("Semenov").house(3).city("Yekaterinburg").birthday(birthday).build());
    h2Db.insert(Person.builder().id(23).firstname("Egor").lastname("Semenov").house(3).city("Ufa").birthday(birthday).build());
    h2Db.insert(Person.builder().id(24).firstname("Egor").lastname("Semenov").house(4).city("Ufa").birthday(birthday).build());
    h2Db.insert(Person.builder().id(25).firstname("Egor").lastname("Semenov").house(4).city("Moscow").birthday(birthday).build());
    data06 = h2Db.getData("SELECT * FROM person WHERE id=15 OR id=16 OR id=17 OR id=18 OR id=19 OR id=20"
        + " OR id=21 OR id=22 OR id=23 OR id=24 OR id=25");

    h2Db.insert(Person.builder().id(26).firstname("Ivan").lastname("Ivanov").house(1).city("Moscow").birthday(birthday).build());
    h2Db.insert(Person.builder().id(26).firstname("Ivan").lastname("Ivanov").house(2).city("Moscow").birthday(birthday).build());
    h2Db.insert(Person.builder().id(27).firstname("Ivan").lastname("Ivanov").house(3).city("Moscow").birthday(birthday).build());
    data07 = h2Db.getData("SELECT * FROM person WHERE id=26 OR id=27");

    h2Db.insert(Person.builder().id(46901).firstname("Test").lastname("Test").house(1).city("Test").birthday(birthday).build());
    h2Db.insert(Person.builder().id(46901).firstname("Test").lastname("Test").house(1).city("Test").birthday(birthday).build());
    h2Db.insert(Person.builder().id(46901).firstname("Test").lastname("Test").house(1).city("Test").birthday(birthday).build());
    data08 = h2Db.getData("SELECT * FROM person WHERE id=46901 OR id=46901");

    executorService = Executors.newFixedThreadPool(20);
  }

  protected void putDataDirect(Map<String, SType> csTypeMap) {
    putDataDirect(csTypeMap, true);
  }

  protected BdbAndDBase getBdbAndDBaseUnique() throws IOException {
    Path path = createUniqueTempDir();
    String directory = path.toAbsolutePath().normalize().toFile().getAbsolutePath();

    BerkleyDB berkleyDB = new BerkleyDB(directory, true);

    DBaseConfig dBaseConfig = new DBaseConfig().setConfigDirectory(directory);
    DBase dBase = new DBase(dBaseConfig, berkleyDB.getStore());

    return new BdbAndDBase(dBase.getDStore(), berkleyDB);
  }

  public static Path createUniqueTempDir() throws IOException {
    return Files.createTempDirectory("junit_dimension_db_" + UUID.randomUUID());
  }

  protected void putDataDirect(Map<String, SType> csTypeMap, boolean isCompressed) {
    dStore = dBase.getDStore();

    cProfilesH2Db = h2Db.getCProfileList().stream()
            .map(cProfile -> cProfile.toBuilder()
                    .colId(cProfile.getColId())
                    .colName(cProfile.getColName())
                    .colDbTypeName(cProfile.getColDbTypeName())
                    .colSizeDisplay(cProfile.getColSizeDisplay())
                    .csType(CSType.builder()
                            .isTimeStamp(cProfile.getColName().equalsIgnoreCase("ID"))
                            .sType(csTypeMap.get(cProfile.getColName()))
                            .cType(cProfile.getColName().equalsIgnoreCase("ID") ? CType.LONG : Mapper.isCType(cProfile))
                            .dType(Mapper.isDBType(cProfile))
                            .build())
                    .build()).toList();

    try {
      SProfile sProfile = new SProfile();
      sProfile.setTableName(tableName);
      sProfile.setTableType(TType.TIME_SERIES);
      sProfile.setIndexType(IType.GLOBAL);
      sProfile.setAnalyzeType(AType.ON_LOAD);
      sProfile.setBackendType(BType.BERKLEYDB);
      sProfile.setCompression(isCompressed);
      sProfile.setCsTypeMap(new LinkedHashMap<>());

      csTypeMap.forEach((colName, sType) -> {
        if (colName.equals("ID")) {
          sProfile.getCsTypeMap().put(colName, new CSType().toBuilder()
              .isTimeStamp(true)
              .sType(sType)
              .cType(cProfilesH2Db.stream().filter(f -> f.getColName().equals(colName)).findAny().orElseThrow().getCsType().getCType())
              .dType(Mapper.isDBType(getCProfile(h2Db.getCProfileList(), colName)))
              .build());
        } else {
          sProfile.getCsTypeMap().put(colName, new CSType().toBuilder()
              .sType(sType)
              .cType(cProfilesH2Db.stream().filter(f -> f.getColName().equals(colName)).findAny().orElseThrow().getCsType().getCType())
              .dType(Mapper.isDBType(getCProfile(h2Db.getCProfileList(), colName)))
              .build());
        }
      });

      try {
        tProfile = dStore.loadDirectTableMetadata(sProfile);
      } catch (TableNameEmptyException e) {
        throw new RuntimeException(e);
      }

      String tableName = tProfile.getTableName();
      dStore.putDataDirect(tableName, data01);
      dStore.putDataDirect(tableName, data02);
      dStore.putDataDirect(tableName, data03);
      dStore.putDataDirect(tableName, data04);
      dStore.putDataDirect(tableName, data05);
      dStore.putDataDirect(tableName, data06);
      dStore.putDataDirect(tableName, data07);
      dStore.putDataDirect(tableName, data08);
    } catch (SqlColMetadataException | EnumByteExceedException e) {
      throw new RuntimeException(e);
    }
  }

  protected void putDataJdbc(Map<String, SType> csTypeMap,
                             TType tableType,
                             IType indexType,
                             AType analyzeType,
                             Boolean compression) {
    dStore = dBase.getDStore();

    cProfilesH2Db = h2Db.getCProfileList().stream()
        .map(cProfile -> cProfile.toBuilder()
            .colId(cProfile.getColId())
            .colName(cProfile.getColName())
            .colDbTypeName(cProfile.getColDbTypeName())
            .colSizeDisplay(cProfile.getColSizeDisplay())
            .csType(CSType.builder()
                .isTimeStamp(cProfile.getColName().equalsIgnoreCase("ID"))
                .sType(csTypeMap.get(cProfile.getColName()))
                .cType(cProfile.getColName().equalsIgnoreCase("ID") ? CType.LONG : Mapper.isCType(cProfile))
                .dType(Mapper.isDBType(cProfile))
                .build())
            .build()).toList();

    try {
      SProfile sProfile = new SProfile();
      sProfile.setTableName(tableName);
      sProfile.setTableType(tableType);
      sProfile.setIndexType(indexType);
      sProfile.setAnalyzeType(analyzeType);
      sProfile.setBackendType(BType.BERKLEYDB);
      sProfile.setCompression(compression);
      sProfile.setCsTypeMap(new LinkedHashMap<>());

      csTypeMap.forEach((colName, sType) -> {
        if (colName.equals("ID")) {
          sProfile.getCsTypeMap().put(colName, new CSType().toBuilder()
              .isTimeStamp(true)
              .sType(sType)
              .cType(cProfilesH2Db.stream().filter(f -> f.getColName().equals(colName)).findAny().orElseThrow().getCsType().getCType())
              .dType(Mapper.isDBType(getCProfile(h2Db.getCProfileList(), colName)))
              .build());
        } else {
          sProfile.getCsTypeMap().put(colName, new CSType().toBuilder()
              .sType(sType)
              .cType(cProfilesH2Db.stream().filter(f -> f.getColName().equals(colName)).findAny().orElseThrow().getCsType().getCType())
              .dType(Mapper.isDBType(getCProfile(h2Db.getCProfileList(), colName)))
              .build());
        }
      });

      try {
        tProfile = dStore.loadJdbcTableMetadata(dbConnection, select, sProfile);
      } catch (TableNameEmptyException e) {
        throw new RuntimeException(e);
      }

      h2Db.putDataJdbc(dStore, tProfile,
                       "SELECT * FROM person WHERE id=1 OR id=2 OR id=3 OR id=4 OR id=5 OR id=6");
      h2Db.putDataJdbc(dStore, tProfile,
                       "SELECT * FROM person WHERE id=7");
      h2Db.putDataJdbc(dStore, tProfile,
                       "SELECT * FROM person WHERE id=10 OR id=8 OR id=9");
      h2Db.putDataJdbc(dStore, tProfile,
                       "SELECT * FROM person WHERE id=11");
      h2Db.putDataJdbc(dStore, tProfile,
                       "SELECT * FROM person WHERE id=12 OR id=13 OR id=14");
      h2Db.putDataJdbc(dStore, tProfile,
                       "SELECT * FROM person WHERE id=15 OR id=16 OR id=17 OR id=18 OR id=19 OR id=20"
              + " OR id=21 OR id=22 OR id=23 OR id=24 OR id=25");
      h2Db.putDataJdbc(dStore, tProfile,
                       "SELECT * FROM person WHERE id=26 OR id=27");

    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  protected SProfile loadData(LoadDataType loadDataType,
                              DStore dStore,
                              String tableName,
                              Map<String, SType> csTypeMap,
                              TType tableType,
                              IType indexType,
                              AType analyzeType,
                              BType backendType,
                              Boolean compression) {
    SProfile sProfile;

    try {
      sProfile = new SProfile();
      sProfile.setTableName(tableName);
      sProfile.setTableType(tableType);
      sProfile.setIndexType(indexType);
      sProfile.setAnalyzeType(analyzeType);
      sProfile.setBackendType(backendType);
      sProfile.setCompression(compression);
      sProfile.setCsTypeMap(packToCSType(h2Db.getCProfileList(), csTypeMap));

      if (loadDataType == LoadDataType.DIRECT) {
        loadDataDirect(dStore, sProfile);
      } else if (loadDataType == LoadDataType.JDBC) {
        loadDataJdbc(dStore, sProfile);
      } else if (loadDataType == LoadDataType.JDBC_BATCH) {
        loadDataJdbcBatch(dStore, sProfile);
      } else {
        throw new RuntimeException("Not supported mode: " + loadDataType);
      }

    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return sProfile;
  }

  protected Map<String, CSType> packToCSType(List<CProfile> cProfileList, Map<String, SType> columnSTypeMap) {
    Map<String, CSType> columnCSTypeMap = new LinkedHashMap<>();

    columnSTypeMap.forEach((colName, sType) -> {
      if (colName.equals("ID")) {
        columnCSTypeMap.put(colName, new CSType().toBuilder()
            .isTimeStamp(true)
            .sType(sType)
            .cType(CType.LONG)
            .dType(Mapper.isDBType(getCProfile(cProfileList, colName)))
            .build());
      } else {
        columnCSTypeMap.put(colName, new CSType().toBuilder()
            .sType(sType)
            .cType(Mapper.isCType(getCProfile(cProfileList, colName)))
            .dType(Mapper.isDBType(getCProfile(cProfileList, colName)))
            .build());
      }
    });

    return columnCSTypeMap;
  }

  private CProfile getCProfile(List<CProfile> cProfileList, String colName) {
    return cProfileList.stream()
        .filter(cProfile -> cProfile.getColName().equalsIgnoreCase(colName))
        .findFirst().orElseThrow();
  }

  private void loadDataDirect(DStore dStore, SProfile sProfile) throws EnumByteExceedException, SqlColMetadataException {
    try {
      tProfile = dStore.loadDirectTableMetadata(sProfile);
    } catch (TableNameEmptyException e) {
      throw new RuntimeException(e);
    }

    String tableName = tProfile.getTableName();
    dStore.putDataDirect(tableName, data01);
    dStore.putDataDirect(tableName, data02);
    dStore.putDataDirect(tableName, data03);
    dStore.putDataDirect(tableName, data04);
    dStore.putDataDirect(tableName, data05);
    dStore.putDataDirect(tableName, data06);
    dStore.putDataDirect(tableName, data07);
    dStore.putDataDirect(tableName, data08);
  }

  private void loadDataJdbc(DStore dStore, SProfile sProfile) throws SQLException {
    TProfile tProfile;

    try {
      tProfile = dStore.loadJdbcTableMetadata(dbConnection, select, sProfile);
    } catch (TableNameEmptyException e) {
      throw new RuntimeException(e);
    }

    h2Db.putDataJdbc(dStore, tProfile,
                     "SELECT * FROM person WHERE id=1 OR id=2 OR id=3 OR id=4 OR id=5 OR id=6");
    h2Db.putDataJdbc(dStore, tProfile,
                     "SELECT * FROM person WHERE id=7");
    h2Db.putDataJdbc(dStore, tProfile,
                     "SELECT * FROM person WHERE id=10 OR id=8 OR id=9");
    h2Db.putDataJdbc(dStore, tProfile,
                     "SELECT * FROM person WHERE id=11");
    h2Db.putDataJdbc(dStore, tProfile,
                     "SELECT * FROM person WHERE id=12 OR id=13 OR id=14");
    h2Db.putDataJdbc(dStore, tProfile,
                     "SELECT * FROM person WHERE id=15 OR id=16 OR id=17 OR id=18 OR id=19 OR id=20"
                         + " OR id=21 OR id=22 OR id=23 OR id=24 OR id=25");
    h2Db.putDataJdbc(dStore, tProfile,
                     "SELECT * FROM person WHERE id=26 OR id=27");
  }

  private void loadDataJdbcBatch(DStore dStore, SProfile sProfile) throws SQLException {
    TProfile tProfile;

    try {
      tProfile = dStore.loadJdbcTableMetadata(dbConnection, select, sProfile);
    } catch (TableNameEmptyException e) {
      throw new RuntimeException(e);
    }

    Integer fBaseBatchSize = 3;
    h2Db.putDataJdbcBatch(dStore, tProfile,
                          "SELECT * FROM person WHERE id=1 OR id=2 OR id=3 OR id=4 OR id=5 OR id=6", fBaseBatchSize);
    h2Db.putDataJdbcBatch(dStore, tProfile,
                          "SELECT * FROM person WHERE id=7", fBaseBatchSize);
    h2Db.putDataJdbcBatch(dStore, tProfile,
                          "SELECT * FROM person WHERE id=10 OR id=8 OR id=9", fBaseBatchSize);
    h2Db.putDataJdbcBatch(dStore, tProfile,
                          "SELECT * FROM person WHERE id=11", fBaseBatchSize);
    h2Db.putDataJdbcBatch(dStore, tProfile,
                          "SELECT * FROM person WHERE id=12 OR id=13 OR id=14", fBaseBatchSize);
    h2Db.putDataJdbcBatch(dStore, tProfile,
                          "SELECT * FROM person WHERE id=15 OR id=16 OR id=17 OR id=18 OR id=19 OR id=20"
                              + " OR id=21 OR id=22 OR id=23 OR id=24 OR id=25", fBaseBatchSize);
    h2Db.putDataJdbcBatch(dStore, tProfile,
                          "SELECT * FROM person WHERE id=26 OR id=27", fBaseBatchSize);
    h2Db.putDataJdbcBatch(dStore, tProfile,
                          "SELECT * FROM person WHERE id=46901 OR id=46901", fBaseBatchSize);
  }

  protected void whenListStackedColumnLastFirstName(DStore dStore,
                                                    TProfile tProfile,
                                                    BeginAndEnd<Integer, Integer> key,
                                                    Map<BeginAndEnd<Integer, Integer>, BiConsumer<List<StackedColumn>, List<StackedColumn>>> givenThenMap)
      throws BeginEndWrongOrderException, SqlColMetadataException {
    List<StackedColumn> lastName = listStackedColumn(dStore, tProfile, "LASTNAME", key.getBegin(), key.getEnd());
    List<StackedColumn> firstName = listStackedColumn(dStore, tProfile, "FIRSTNAME", key.getBegin(), key.getEnd());

    BiConsumer<List<StackedColumn>, List<StackedColumn>> givenThenFunction = givenThenMap.get(key);

    if (givenThenFunction != null) {
      givenThenFunction.accept(lastName, firstName);
    } else {
      throw new RuntimeException("No assertion function found for the given begin and end pair.");
    }
  }

  protected void whenListStackedColumnFilter(DStore dStore,
                                             TProfile tProfile,
                                             FSColAndRange key,
                                             Map<FSColAndRange, Consumer<List<StackedColumn>>> givenThenMap)
      throws BeginEndWrongOrderException, SqlColMetadataException {
    FilterColumn fsColumn = (FilterColumn) key.getParameters();
    BeginAndEnd<Integer, Integer> range = (BeginAndEnd<Integer, Integer>) key.getRange();

    List<StackedColumn> stackedColumnFilter = getListStackedDataBySqlColFilter(dStore,
                                                                               tProfile,
                                                                               tProfile.getCProfiles(),
                                                                               fsColumn.getColumnName(),
                                                                               fsColumn.getColumnNameFilter(),
                                                                               fsColumn.getFilterValue(),
                                                                               range.getBegin(),
                                                                               range.getEnd());

    Consumer<List<StackedColumn>> givenThenFunction = givenThenMap.get(key);

    if (givenThenFunction != null) {
      givenThenFunction.accept(stackedColumnFilter);
    } else {
      throw new RuntimeException("No assertion function found for the given begin and end pair.");
    }
  }

  protected void whenListGanttColumn(DStore dStore,
                                     TProfile tProfile,
                                     FSColAndRange key,
                                     Map<FSColAndRange, Consumer<List<GanttColumnCount>>> givenThenMap)
      throws BeginEndWrongOrderException, SqlColMetadataException, GanttColumnNotSupportedException {
    FSColumn fsColumn = (FSColumn) key.getParameters();
    BeginAndEnd<Integer, Integer> range = (BeginAndEnd<Integer, Integer>) key.getRange();

    List<GanttColumnCount> ganttColumnCount = getDataGanttColumn(dStore,
                                                                 tProfile,
                                                                 fsColumn.getFirstCol(),
                                                                 fsColumn.getSecondCol(),
                                                                 range.getBegin(),
                                                                 range.getEnd());

    Consumer<List<GanttColumnCount>> givenThenFunction = givenThenMap.get(key);

    if (givenThenFunction != null) {
      givenThenFunction.accept(ganttColumnCount);
    } else {
      throw new RuntimeException("No assertion function found for the given begin and end pair.");
    }
  }

  protected void whenRawExpectedActual(DStore dStore,
                                       TProfile tProfile,
                                       BeginAndEnd<Integer, Integer> key,
                                       Map<BeginAndEnd<Integer, Integer>, BiConsumer<List<List<Object>>, List<List<Object>>>> givenThenMap) {
    List<List<Object>> expected = new ArrayList<>();
    loadExpected(expected);

    List<List<Object>> actual = getRawDataAll(dStore, tProfile, key.getBegin(), key.getEnd());

    BiConsumer<List<List<Object>>, List<List<Object>>> givenThenFunction = givenThenMap.get(key);

    if (givenThenFunction != null) {
      givenThenFunction.accept(expected, actual);
    } else {
      throw new RuntimeException("No assertion function found for the given begin and end pair.");
    }
  }

  protected void whenRawExpectedActualFilter(DStore dStore,
                                             TProfile tProfile,
                                             FSColAndRange key,
                                             Map<FSColAndRange, BiConsumer<List<List<Object>>, List<List<Object>>>> givenThenMap) {
    List<List<Object>> expected = new ArrayList<>();
    loadExpected(expected);

    String colName = (String) key.getParameters();
    CProfile cProfile = getCProfileByColumnName(tProfile.getCProfiles(), colName);
    BeginAndEnd<Integer, Integer> range = (BeginAndEnd<Integer, Integer>) key.getRange();
    List<List<Object>> actual = getRawDataByColumn(dStore, tProfile, cProfile, range.getBegin(), range.getEnd());

    BiConsumer<List<List<Object>>, List<List<Object>>> givenThenFunction = givenThenMap.get(key);

    if (givenThenFunction != null) {
      givenThenFunction.accept(expected, actual);
    } else {
      throw new RuntimeException("No assertion function found for the given begin and end pair.");
    }
  }

  protected void whenRawExpectedActualRSFilter(DStore dStore,
                                               TProfile tProfile,
                                               FSColAndRange key,
                                               Boolean checkEmptyRS,
                                               Map<FSColAndRange, BiConsumer<List<List<Object>>, List<List<Object>>>> givenThenMap) {
    List<List<Object>> expected = new ArrayList<>();
    loadExpected(expected);

    Integer fetchSize = (Integer) key.getParameters();
    BeginAndEnd<Integer, Integer> range = (BeginAndEnd<Integer, Integer>) key.getRange();
    List<List<Object>> actual;
    if (checkEmptyRS) {
      actual = getRawDataFromResultSetEmptyCheck(dStore, tProfile, range.getBegin(), range.getEnd(), fetchSize);
    } else {
      actual = getRawDataFromResultSet(dStore, tProfile, range.getBegin(), range.getEnd(), fetchSize);
    }

    BiConsumer<List<List<Object>>, List<List<Object>>> givenThenFunction = givenThenMap.get(key);

    if (givenThenFunction != null) {
      givenThenFunction.accept(expected, actual);
    } else {
      throw new RuntimeException("No assertion function found for the given begin and end pair.");
    }
  }

  protected List<StackedColumn> listStackedColumn(DStore dStore,
                                                  TProfile tProfile,
                                                  String colName,
                                                  int begin,
                                                  int end)
      throws BeginEndWrongOrderException, SqlColMetadataException {
    return dStore.getStacked(tProfile.getTableName(),
                             tProfile.getCProfiles().stream()
                                               .filter(k -> k.getColName().equalsIgnoreCase(colName))
                                               .findAny()
                                               .orElseThrow(),
                             GroupFunction.COUNT,
                             null,
                             begin,
                             end);
  }

  protected List<GanttColumnCount> getDataGanttColumn(DStore dStore,
                                                      TProfile tProfile,
                                                      String firstColName,
                                                      String secondColName,
                                                      int begin,
                                                      int end)
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    CProfile firstLevelGroupBy = tProfile.getCProfiles().stream()
        .filter(k -> k.getColName().equalsIgnoreCase(firstColName))
        .findAny().orElseThrow();
    CProfile secondLevelGroupBy = tProfile.getCProfiles().stream()
        .filter(k -> k.getColName().equalsIgnoreCase(secondColName))
        .findAny().orElseThrow();

    return getListGanttColumnTwoLevelGrouping(dStore, tProfile, firstLevelGroupBy, secondLevelGroupBy, begin, end);
  }

  protected void putDataJdbcBatch(Map<String, SType> csTypeMap) {
    dStore = dBase.getDStore();

    cProfilesH2Db = h2Db.getCProfileList().stream()
        .map(col -> col.toBuilder()
            .colId(col.getColId())
            .colName(col.getColName())
            .colDbTypeName(col.getColDbTypeName())
            .colSizeDisplay(col.getColSizeDisplay())
            .csType(CSType.builder()
                .isTimeStamp(col.getColName().equalsIgnoreCase("ID"))
                .sType(csTypeMap.get(col.getColName()))
                .cType(col.getColName().equalsIgnoreCase("ID") ? CType.LONG : Mapper.isCType(col))
                .build())
            .build()).toList();

    try {
      SProfile sProfile = new SProfile();
      sProfile.setTableName(tableName);
      sProfile.setTableType(TType.TIME_SERIES);
      sProfile.setIndexType(IType.GLOBAL);
      sProfile.setAnalyzeType(AType.ON_LOAD);
      sProfile.setBackendType(BType.BERKLEYDB);
      sProfile.setCompression(true);
      sProfile.setCsTypeMap(new HashMap<>());

      csTypeMap.forEach((colName, sType) -> {
        if (colName.equals("ID")) {
          sProfile.getCsTypeMap().put(colName, new CSType().toBuilder()
              .isTimeStamp(true)
              .sType(sType)
              .cType(CType.LONG)
              .dType(Mapper.isDBType(getCProfile(h2Db.getCProfileList(), colName)))
              .build());
        } else {
          sProfile.getCsTypeMap().put(colName, new CSType().toBuilder()
              .sType(sType)
              .cType(Mapper.isCType(getCProfile(h2Db.getCProfileList(), colName)))
              .dType(Mapper.isDBType(getCProfile(h2Db.getCProfileList(), colName)))
              .build());
        }
      });

      try {
        tProfile = dStore.loadJdbcTableMetadata(dbConnection, select, sProfile);
      } catch (TableNameEmptyException e) {
        throw new RuntimeException(e);
      }

      Integer fBaseBatchSize = 3;
      h2Db.putDataJdbcBatch(dStore, tProfile,
                            "SELECT * FROM person WHERE id=1 OR id=2 OR id=3 OR id=4 OR id=5 OR id=6", fBaseBatchSize);
      h2Db.putDataJdbcBatch(dStore, tProfile,
                            "SELECT * FROM person WHERE id=7", fBaseBatchSize);
      h2Db.putDataJdbcBatch(dStore, tProfile,
                            "SELECT * FROM person WHERE id=10 OR id=8 OR id=9", fBaseBatchSize);
      h2Db.putDataJdbcBatch(dStore, tProfile,
                            "SELECT * FROM person WHERE id=11", fBaseBatchSize);
      h2Db.putDataJdbcBatch(dStore, tProfile,
                            "SELECT * FROM person WHERE id=12 OR id=13 OR id=14", fBaseBatchSize);
      h2Db.putDataJdbcBatch(dStore, tProfile,
                            "SELECT * FROM person WHERE id=15 OR id=16 OR id=17 OR id=18 OR id=19 OR id=20"
              + " OR id=21 OR id=22 OR id=23 OR id=24 OR id=25", fBaseBatchSize);
      h2Db.putDataJdbcBatch(dStore, tProfile,
                            "SELECT * FROM person WHERE id=26 OR id=27", fBaseBatchSize);
      h2Db.putDataJdbcBatch(dStore, tProfile,
                            "SELECT * FROM person WHERE id=46901 OR id=46901", fBaseBatchSize);

    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  protected void loadExpected(List<List<Object>> expected) {
    expected.add(Arrays.asList(new String[]{"1", "Alex", "Ivanov", "1", "Moscow", "01.01.2023 01:01:01"}));
    expected.add(Arrays.asList(new String[]{"2", "Ivan", "Ivanov", "2", "Moscow", "01.01.2023 01:01:01"}));
    expected.add(Arrays.asList(new String[]{"3", "Oleg", "Petrov", "1", "Moscow", "01.01.2023 01:01:01"}));
    expected.add(Arrays.asList(new String[]{"4", "Lee", "Sui", "1", "Moscow", "01.01.2023 01:01:01"}));
    expected.add(Arrays.asList(new String[]{"5", "Lee", "Ivanov", "1", "Moscow", "01.01.2023 01:01:01"}));
    expected.add(Arrays.asList(new String[]{"6", "Lee", "Ivanov", "2", "Moscow", "01.01.2023 01:01:01"}));
    expected.add(Arrays.asList(new String[]{"7", "Men", "Petrov", "1", "Yekaterinburg", "01.01.2023 01:01:01"}));
    expected.add(Arrays.asList(new String[]{"8", "Ion", "Тихий", "1", "Moscow", "01.01.2023 01:01:01"}));
    expected.add(Arrays.asList(new String[]{"9", "Федор", "Шаляпин", "1", "Moscow", "01.01.2023 01:01:01"}));
    expected.add(Arrays.asList(new String[]{"10", "Петр", "Пирогов", "1", "Moscow", "01.01.2023 01:01:01"}));
    expected.add(Arrays.asList(new String[]{"11", "Oleg", "Semenov", "1", "Moscow", "01.01.2023 01:01:01"}));
    expected.add(Arrays.asList(new String[]{"12", "Oleg", "Mirko", "2", "Yekaterinburg", "01.01.2023 01:01:01"}));
  }

  protected List<GanttColumnCount> getDataGanttColumn(String firstColName, String secondColName, int begin, int end)
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    CProfile firstLevelGroupBy = tProfile.getCProfiles().stream()
        .filter(k -> k.getColName().equalsIgnoreCase(firstColName))
        .findAny().get();
    CProfile secondLevelGroupBy = tProfile.getCProfiles().stream()
        .filter(k -> k.getColName().equalsIgnoreCase(secondColName))
        .findAny().get();

    return getListGanttColumnTwoLevelGrouping(dStore, tProfile, firstLevelGroupBy, secondLevelGroupBy, begin, end);
  }

  public List<GanttColumnCount> getListGanttColumnTwoLevelGrouping(DStore dStore, TProfile tProfile,
                                                                   CProfile firstLevelGroupBy, CProfile secondLevelGroupBy, long begin, long end)
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    return dStore.getGanttCount(tProfile.getTableName(), firstLevelGroupBy, secondLevelGroupBy, null, begin, end);
  }

  protected void assertForGanttColumn(List<GanttColumnCount> expected,
                                      List<GanttColumnCount> actual) {
    assertEquals(expected.size(), actual.size(),"Number of GanttColumnCount entries differs");

    Map<String, Map<String, Integer>> expectedMap = expected.stream()
        .collect(Collectors.toMap(GanttColumnCount::getKey, GanttColumnCount::getGantt));

    Map<String, Map<String, Integer>> actualMap = actual.stream()
        .collect(Collectors.toMap(GanttColumnCount::getKey, GanttColumnCount::getGantt));

    assertEquals(expectedMap.keySet(), actualMap.keySet(), "Keys don't match");

    expectedMap.forEach((key, expectedGantt) -> {
      Map<String, Integer> actualGantt = actualMap.get(key);
      assertEquals(expectedGantt, actualGantt,
                   () -> "Gantt values differ for key: " + key);
    });
  }

  public List<StackedColumn> getListStackedDataBySqlCol(DStore dStore,
                                                        TProfile tProfile,
                                                        List<CProfile> cProfiles,
                                                        String colName,
                                                        int begin,
                                                        int end)
      throws BeginEndWrongOrderException, SqlColMetadataException {
    return dStore.getStacked(tProfile.getTableName(), cProfiles.stream()
        .filter(k -> k.getColName().equalsIgnoreCase(colName)).findAny().orElseThrow(), GroupFunction.COUNT, null, begin, end);
  }

  public List<StackedColumn> getListStackedDataBySqlColFilter(DStore dStore,
                                                              TProfile tProfile,
                                                              List<CProfile> cProfiles,
                                                              String colName,
                                                              String colNameFilter,
                                                              String filter,
                                                              int begin,
                                                              int end)
      throws BeginEndWrongOrderException, SqlColMetadataException {
    CProfile cProfile = cProfiles.stream()
        .filter(k -> k.getColName().equalsIgnoreCase(colName))
        .findAny()
        .orElseThrow();
    CProfile cProfileFilter = cProfiles.stream()
        .filter(k -> k.getColName().equalsIgnoreCase(colNameFilter))
        .findAny()
        .orElseThrow();

    CompositeFilter compositeFilter = new CompositeFilter(
        List.of(new FilterCondition(
            cProfileFilter,
            new String[]{filter},
            CompareFunction.EQUAL)),
        LogicalOperator.AND);

    return dStore.getStacked(tProfile.getTableName(),
                             cProfile,
                             GroupFunction.COUNT,
                             compositeFilter,
                             begin, end);
  }

  public Object lastListStackedKey(List<StackedColumn> list) {
    return list.stream().reduce((first, second) -> second).orElseThrow()
        .getKeyCount().entrySet().stream().reduce((first, second) -> second).orElseThrow().getKey();
  }

  public Object firstListStackedKey(List<StackedColumn> list) {
    return list.stream().findFirst().orElseThrow()
        .getKeyCount().entrySet().stream().findFirst().orElseThrow().getKey();
  }

  public Object firstListStackedValue(List<StackedColumn> list) {
    return list.stream().findFirst().orElseThrow()
        .getKeyCount().entrySet().stream().findFirst().orElseThrow().getValue();
  }

  public Object firstListStackedValue(List<StackedColumn> list, String key) {
    return list.stream()
        .map(column -> column.getKeyCount().get(key))
        .filter(Objects::nonNull)
        .mapToInt(Integer::intValue)
        .sum();
  }

  public List<StackedColumn> getDataStackedColumn(String colName, int begin, int end)
      throws BeginEndWrongOrderException, SqlColMetadataException {
    return getListStackedDataBySqlCol(dStore, tProfile, tProfile.getCProfiles(), colName, begin, end);
  }

  public List<StackedColumn> getDataStackedColumnFilter(String colName,
                                                        String colNameFilter,
                                                        String filter,
                                                        int begin, int end)
      throws BeginEndWrongOrderException, SqlColMetadataException {
    return getListStackedDataBySqlColFilter(dStore, tProfile, tProfile.getCProfiles(), colName, colNameFilter, filter, begin, end);
  }

  public List<List<Object>> getRawDataAll(int begin, int end) {
    return dStore.getRawDataAll(tProfile.getTableName(), begin, end);
  }

  public List<List<Object>> getRawDataAll(DStore dStore, TProfile tProfile, int begin, int end) {
    return dStore.getRawDataAll(tProfile.getTableName(), begin, end);
  }

  public List<List<Object>> getRawDataByColumn(CProfile cProfile, int begin, int end) {
    return dStore.getRawDataByColumn(tProfile.getTableName(), cProfile, begin, end);
  }

  public List<List<Object>> getRawDataByColumn(DStore dStore, TProfile tProfile, CProfile cProfile, int begin, int end) {
    return dStore.getRawDataByColumn(tProfile.getTableName(), cProfile, begin, end);
  }

  public CProfile getCProfileByColumnName(String colName) {
    return tProfile.getCProfiles().stream().filter(f -> f.getColName().equals(colName)).findAny().orElseThrow();
  }

  public CProfile getCProfileByColumnName(List<CProfile> cProfiles, String colName) {
    return cProfiles.stream().filter(f -> f.getColName().equals(colName)).findAny().orElseThrow();
  }

  public List<StackedColumn> getStackedData(String colName, int begin, int end)
      throws BeginEndWrongOrderException, SqlColMetadataException {
    return getListStackedDataBySqlCol(dStore, tProfile, tProfile.getCProfiles(), colName, begin, end);
  }

  protected void assertForRaw(List<List<Object>> expected, List<List<Object>> actual) {
    for (int i = 0; i < expected.size(); i++) {
      for (int j = 0; j < expected.get(i).size(); j++) {
        assertEquals(String.valueOf(expected.get(i).get(j)), String.valueOf(actual.get(i).get(j)));
      }
    }
  }

  private List<List<Object>> getRawDataFromResultSet(DStore dStore, TProfile tProfile, int begin, int end, int fetchSize) {
    BatchResultSet batchResultSet = dStore.getBatchResultSet(tProfile.getTableName(), begin, end, fetchSize);

    List<List<Object>> actual = new ArrayList<>();
    while (batchResultSet.next()) {
      List<List<Object>> var = batchResultSet.getObject();
      log.info("Output by fetchSize: " + var);

      actual.addAll(var);
    }

    return actual;
  }

  private List<List<Object>> getRawDataFromResultSetEmptyCheck(DStore dStore, TProfile tProfile, int begin, int end, int fetchSize) {
    BatchResultSet batchResultSet = dStore.getBatchResultSet(tProfile.getTableName(), begin, end, fetchSize);

    boolean isEmpty = Boolean.TRUE;

    while (batchResultSet.next()) {
      isEmpty = Boolean.FALSE;
    }

    if (!isEmpty) {
      throw new RuntimeException("Result set next method calling without data, need to fix it!");
    }

    return Collections.emptyList();
  }

  protected static List<Map<String, Object>> generateTestConfigurations(PermutationState permutationState) {
    List<Map<String, Object>> configurations = new ArrayList<>();

    List<Map<String, SType>> permutations = new ArrayList<>();
    generatePersonSTypePermutations(permutations, permutationState);

      for (Map<String, SType> csTypeMap : permutations) {
        log.info("## SType permutation: " + csTypeMap);

        for (Boolean compression : boolArray) {
          for (IType indexType : IType.values()) {
            for (AType analyzeType : AType.values()) {
              for (LoadDataType loadDataType : LoadDataType.values()) {
                Map<String, Object> config = new LinkedHashMap<>();

                config.put("csTypeMap", csTypeMap);
                config.put("indexType", indexType);
                config.put("analyzeType", analyzeType);
                config.put("compression", compression);
                config.put("loadDataType", loadDataType);
                configurations.add(config);
              }
            }
          }
        }
      }

    return configurations;
  }

  private static void generatePersonSTypePermutations(List<Map<String, SType>> result,
                                                      PermutationState permutationState) {
    List<String> fields = Arrays.asList("ID", "FIRSTNAME", "LASTNAME", "HOUSE", "CITY", "BIRTHDAY");

    if (permutationState.equals(PermutationState.NONE)) {
      for (SType sType : SType.values()) {
        Map<String, SType> csTypeMap = new LinkedHashMap<>();
        fields.forEach(colName -> csTypeMap.put(colName, sType));

        csTypeMap.put("ID", SType.RAW);

        result.add(csTypeMap);
      }
    } else if (permutationState.equals(PermutationState.PARTIAL)) {
      for (SType sTypeMain : SType.values()) {

        Map<String, SType> csTypeMapMain = new LinkedHashMap<>();

        fields.forEach(colName -> csTypeMapMain.put(colName, sTypeMain));
        csTypeMapMain.put("ID", SType.RAW);

        fields.forEach(colName -> {
          for (SType sType : SType.values()) {
            Map<String, SType> csTypeMap = new LinkedHashMap<>(csTypeMapMain);
            csTypeMap.put(colName, sType);

            csTypeMap.put("ID", SType.RAW);

            result.add(csTypeMap);
          }
        });
      }
    } else if (permutationState.equals(PermutationState.ALL)) {
      generatePermutations(new LinkedHashMap<>(), fields, 0, result);
    } else {
      throw new RuntimeException("Not supported permutation state:" + permutationState);
    }
  }

  private static void generatePermutations(Map<String, SType> currentMap,
                                           List<String> fields,
                                           int index,
                                           List<Map<String, SType>> result) {
    if (index == fields.size()) {
      result.add(new LinkedHashMap<>(currentMap));
      return;
    }

    String field = fields.get(index);
    for (SType sType : SType.values()) {
      if (field.equalsIgnoreCase("ID")) {
        currentMap.put("ID", SType.RAW);
      } else {
        currentMap.put(field, sType);
      }
      generatePermutations(currentMap, fields, index + 1, result);
    }
  }

  @AfterAll
  public void closeDb() throws SQLException, IOException, InterruptedException {
    executorService.shutdown();
    if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
      executorService.shutdownNow();
    }

    berkleyDB.closeDatabase();
    berkleyDB.removeDirectory();

    h2Db.execute("DROP ALL OBJECTS");
  }
}
