package ru.dimension.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.extern.log4j.Log4j2;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import ru.dimension.db.common.mode.JDBCMode;
import ru.dimension.db.exception.BeginEndWrongOrderException;
import ru.dimension.db.exception.GanttColumnNotSupportedException;
import ru.dimension.db.exception.SqlColMetadataException;
import ru.dimension.db.exception.TableNameEmptyException;
import ru.dimension.db.model.CompareFunction;
import ru.dimension.db.model.DBaseTestConfig;
import ru.dimension.db.model.GroupFunction;
import ru.dimension.db.model.Manager;
import ru.dimension.db.model.OrderBy;
import ru.dimension.db.model.filter.CompositeFilter;
import ru.dimension.db.model.filter.FilterCondition;
import ru.dimension.db.model.filter.LogicalOperator;
import ru.dimension.db.model.output.GanttColumnCount;
import ru.dimension.db.model.output.GanttColumnSum;
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
import ru.dimension.db.source.H2Database;

@Log4j2
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DBaseUseCasesCodeFilterTest implements JDBCMode {

  @TempDir
  static File databaseDir;
  private DBaseTestConfig configJdbc;
  private H2Database h2Db;
  private TProfile tProfile;
  private CProfile cProfileFirstName;
  private CProfile cProfileLastName;
  private CProfile cProfileCity;
  private CProfile cProfileSalary;
  private CProfile cProfileKpi;
  private CProfile cProfileBirthday;

  private static final String MANAGER_FILTER_TEST_TABLE = "manager_filter_test_table";
  private static final long BEGIN = 1;
  private static final long END = 10;

  @BeforeAll
  public void initialization() throws SQLException, IOException, TableNameEmptyException {
    h2Db = new H2Database("jdbc:h2:mem:manager_filter_test");
    configJdbc = new DBaseTestConfig(databaseDir, h2Db);
    loadManagerDataH2(h2Db);
    tProfile = setupManagerProfile();
    putDataJdbc(configJdbc.getDStore(), tProfile, configJdbc.getH2DbConnection(), "SELECT * FROM manager");
    initializeCProfiles();
  }

  private void loadManagerDataH2(H2Database h2Db) throws SQLException {
    String createTableSql = """
            CREATE TABLE MANAGER (
                id        INT,
                firstname VARCHAR(64),
                lastname  VARCHAR(64),
                house     INT,
                city      VARCHAR(64),
                birthday  TIMESTAMP,
                salary    DOUBLE,
                kpi       FLOAT
            )
            """;
    h2Db.execute(createTableSql);

    LocalDateTime birthday = LocalDateTime.of(2023, 1, 1, 1, 1, 1);
    for (Manager manager : createTestManagers(birthday)) {
      h2Db.insert(manager);
    }
  }

  private List<Manager> createTestManagers(LocalDateTime birthday) {
    return List.of(
        Manager.builder().id(1).firstname("Alex").lastname("Ivanov").house(1).city("Moscow").birthday(birthday).salary(1000.0).kpi(0.8f).build(),
        Manager.builder().id(2).firstname("Ivan").lastname("Ivanov").house(2).city("Moscow").birthday(birthday).salary(2000.0).kpi(0.9f).build(),
        Manager.builder().id(3).firstname("Oleg").lastname("Petrov").house(3).city("Yekaterinburg").birthday(birthday).salary(3000.0).kpi(0.7f).build(),
        Manager.builder().id(4).firstname("Anna").lastname("Sidorova").house(4).city("Novosibirsk").birthday(birthday).salary(4000.0).kpi(0.6f).build(),
        Manager.builder().id(5).firstname("Elena").lastname("Smirnova").house(5).city("Moscow").birthday(birthday).salary(5000.0).kpi(0.5f).build(),
        Manager.builder().id(6).firstname("Dmitry").lastname("Popov").house(6).city("Yekaterinburg").birthday(birthday).salary(6000.0).kpi(0.4f).build(),
        Manager.builder().id(7).firstname("Irina").lastname("Kuznetsova").house(7).city("Novosibirsk").birthday(birthday).salary(7000.0).kpi(0.3f).build(),
        Manager.builder().id(8).firstname("Maxim").lastname("Vasiliev").house(8).city("Moscow").birthday(birthday).salary(8000.0).kpi(0.2f).build(),
        Manager.builder().id(9).firstname("Olga").lastname("Novikova").house(9).city("Yekaterinburg").birthday(birthday).salary(9000.0).kpi(0.1f).build(),
        Manager.builder().id(10).firstname("Sergey").lastname("Fedorov").house(10).city("Novosibirsk").birthday(birthday).salary(10000.0).kpi(0.0f).build()
    );
  }

  private TProfile setupManagerProfile() throws SQLException, TableNameEmptyException {
    SProfile sProfile = getSProfileJdbc(
        MANAGER_FILTER_TEST_TABLE,
        TType.TIME_SERIES,
        IType.LOCAL,
        AType.ON_LOAD,
        BType.BERKLEYDB,
        true
    );
    sProfile.getCsTypeMap().put("ID", CSType.builder().isTimeStamp(true).sType(SType.RAW).cType(CType.LONG).build());
    sProfile.getCsTypeMap().put("SALARY", CSType.builder().sType(SType.RAW).cType(CType.DOUBLE).build());
    sProfile.getCsTypeMap().put("KPI", CSType.builder().sType(SType.RAW).cType(CType.FLOAT).build());

    return configJdbc.getDStore().loadJdbcTableMetadata(
        configJdbc.getH2DbConnection(),
        "SELECT * FROM MANAGER",
        sProfile
    );
  }

  private void initializeCProfiles() {
    cProfileFirstName = getCProfileByName("FIRSTNAME");
    cProfileLastName = getCProfileByName("LASTNAME");
    cProfileCity = getCProfileByName("CITY");
    cProfileSalary = getCProfileByName("SALARY");
    cProfileKpi = getCProfileByName("KPI");
    cProfileBirthday = getCProfileByName("BIRTHDAY");
  }

  private CProfile getCProfileByName(String colName) {
    return tProfile.getCProfiles().stream()
        .filter(f -> f.getColName().equals(colName))
        .findFirst().orElseThrow();
  }

  @Test
  public void distinctValuesForStringColumns() throws BeginEndWrongOrderException {
    List<String> actualFirstNameDesc = configJdbc.getDStore()
        .getDistinct(tProfile.getTableName(), cProfileFirstName, OrderBy.DESC, null, 10, BEGIN, END);
    List<String> actualLastNameDesc = configJdbc.getDStore()
        .getDistinct(tProfile.getTableName(), cProfileLastName, OrderBy.DESC, null, 10, BEGIN, END);
    List<String> actualFirstNameAsc = configJdbc.getDStore()
        .getDistinct(tProfile.getTableName(), cProfileFirstName, OrderBy.ASC, null, 10, BEGIN, END);
    List<String> actualLastNameAsc = configJdbc.getDStore()
        .getDistinct(tProfile.getTableName(), cProfileLastName, OrderBy.ASC, null, 10, BEGIN, END);

    assertEquals(List.of("Sergey", "Olga", "Oleg", "Maxim", "Ivan", "Irina", "Elena", "Dmitry", "Anna", "Alex"), actualFirstNameDesc);
    assertEquals(List.of("Vasiliev", "Smirnova", "Sidorova", "Popov", "Petrov", "Novikova", "Kuznetsova", "Ivanov", "Fedorov"), actualLastNameDesc);
    assertEquals(List.of("Alex", "Anna", "Dmitry", "Elena", "Irina", "Ivan", "Maxim", "Oleg", "Olga", "Sergey"), actualFirstNameAsc);
    assertEquals(List.of("Fedorov", "Ivanov", "Kuznetsova", "Novikova", "Petrov", "Popov", "Sidorova", "Smirnova", "Vasiliev"), actualLastNameAsc);
  }

  @Test
  public void distinctValuesForNumericColumns() throws BeginEndWrongOrderException {
    List<String> actualSalaryAsc = configJdbc.getDStore()
        .getDistinct(tProfile.getTableName(), cProfileSalary, OrderBy.ASC, null, 10, BEGIN, END);
    List<String> actualSalaryDesc = configJdbc.getDStore()
        .getDistinct(tProfile.getTableName(), cProfileSalary, OrderBy.DESC, null, 10, BEGIN, END);
    List<String> actualKpiAsc = configJdbc.getDStore()
        .getDistinct(tProfile.getTableName(), cProfileKpi, OrderBy.ASC, null, 10, BEGIN, END);
    List<String> actualKpiDesc = configJdbc.getDStore()
        .getDistinct(tProfile.getTableName(), cProfileKpi, OrderBy.DESC, null, 10, BEGIN, END);

    assertEquals(List.of("1000.0", "2000.0", "3000.0", "4000.0", "5000.0", "6000.0", "7000.0", "8000.0", "9000.0", "10000.0"), actualSalaryAsc);
    assertEquals(List.of("10000.0", "9000.0", "8000.0", "7000.0", "6000.0", "5000.0", "4000.0", "3000.0", "2000.0", "1000.0"), actualSalaryDesc);
    assertEquals(List.of("0.0", "0.1", "0.2", "0.3", "0.4", "0.5", "0.6", "0.7", "0.8", "0.9"), actualKpiAsc);
    assertEquals(List.of("0.9", "0.8", "0.7", "0.6", "0.5", "0.4", "0.3", "0.2", "0.1", "0.0"), actualKpiDesc);
  }

  @Test
  public void distinctWithCompositeFilter() throws BeginEndWrongOrderException {
    CompositeFilter salaryFilter = new CompositeFilter(
        List.of(new FilterCondition(cProfileSalary, new String[]{"3000.0", "7000.0"}, CompareFunction.EQUAL)),
        LogicalOperator.OR);
    List<String> salaryWithFilter = configJdbc.getDStore()
        .getDistinct(tProfile.getTableName(), cProfileSalary, OrderBy.ASC, salaryFilter, 10, BEGIN, END);
    assertEquals(List.of("3000.0", "7000.0"), salaryWithFilter);

    CompositeFilter kpiRangeFilter = new CompositeFilter(
        List.of(new FilterCondition(cProfileKpi, new String[]{"0.3"}, CompareFunction.EQUAL)),
        LogicalOperator.AND);
    List<String> kpiWithRangeFilter = configJdbc.getDStore()
        .getDistinct(tProfile.getTableName(), cProfileKpi, OrderBy.ASC, kpiRangeFilter, 10, BEGIN, END);
    assertEquals(List.of("0.3"), kpiWithRangeFilter);
  }

  @Test
  public void distinctWithFilterConditions() throws BeginEndWrongOrderException {
    CompositeFilter cityFilter = new CompositeFilter(
        List.of(new FilterCondition(cProfileCity, new String[]{"Moscow", "Novosibirsk"}, CompareFunction.EQUAL)),
        LogicalOperator.AND);
    List<String> citiesWithFilter = configJdbc.getDStore()
        .getDistinct(tProfile.getTableName(), cProfileCity, OrderBy.ASC, cityFilter, 10, BEGIN, END);
    assertEquals(new HashSet<>(List.of("Moscow", "Novosibirsk")), new HashSet<>(citiesWithFilter));

    CompositeFilter salaryRangeFilter = new CompositeFilter(
        List.of(new FilterCondition(cProfileSalary, new String[]{"5000.0"}, CompareFunction.EQUAL)),
        LogicalOperator.AND);
    List<String> highSalaryNames = configJdbc.getDStore()
        .getDistinct(tProfile.getTableName(), cProfileFirstName, OrderBy.ASC, salaryRangeFilter, 10, BEGIN, END);
    assertEquals(new HashSet<>(List.of("Elena")), new HashSet<>(highSalaryNames));
  }

  @Test
  public void distinctWithComplexFilters() throws BeginEndWrongOrderException {
    CompositeFilter complexFilter = new CompositeFilter(
        List.of(
            new FilterCondition(cProfileCity, new String[]{"Yekaterinburg"}, CompareFunction.EQUAL),
            new FilterCondition(cProfileSalary, new String[]{"3000.0"}, CompareFunction.EQUAL)
        ),
        LogicalOperator.AND
    );
    List<String> filteredCities = configJdbc.getDStore()
        .getDistinct(tProfile.getTableName(), cProfileCity, OrderBy.ASC, complexFilter, 10, BEGIN, END);
    assertEquals(List.of("Yekaterinburg"), filteredCities);
  }

  @Test
  public void distinctWithEmptyFilterList() throws BeginEndWrongOrderException {
    CompositeFilter empty = new CompositeFilter(List.of(), LogicalOperator.AND);
    List<String> res = configJdbc.getDStore()
        .getDistinct(tProfile.getTableName(), cProfileCity, OrderBy.ASC, empty, 100, BEGIN, END);
    assertEquals(List.of("Moscow", "Novosibirsk", "Yekaterinburg"), res);
  }

  @Test
  public void distinctWithSingleCondition() throws BeginEndWrongOrderException {
    CompositeFilter single = new CompositeFilter(
        List.of(new FilterCondition(cProfileCity, new String[]{"Novosibirsk"}, CompareFunction.EQUAL)),
        LogicalOperator.AND
    );
    List<String> res = configJdbc.getDStore()
        .getDistinct(tProfile.getTableName(), cProfileCity, OrderBy.ASC, single, 10, BEGIN, END);
    assertEquals(List.of("Novosibirsk"), res);
  }

  @Test
  public void distinctOrWithNonOverlappingRanges() throws BeginEndWrongOrderException {
    CompositeFilter orFilter = new CompositeFilter(
        List.of(
            new FilterCondition(cProfileSalary, new String[]{"1000.0"}, CompareFunction.EQUAL),
            new FilterCondition(cProfileSalary, new String[]{"10000.0"}, CompareFunction.EQUAL)
        ),
        LogicalOperator.OR
    );
    List<String> res = configJdbc.getDStore()
        .getDistinct(tProfile.getTableName(), cProfileSalary, OrderBy.ASC, orFilter, 10, BEGIN, END);
    assertEquals(List.of("1000.0", "10000.0"), res);
  }

  @Test
  public void distinctAndWithMutuallyExclusive() throws BeginEndWrongOrderException {
    CompositeFilter noMatch = new CompositeFilter(
        List.of(
            new FilterCondition(cProfileCity, new String[]{"Moscow"}, CompareFunction.EQUAL),
            new FilterCondition(cProfileSalary, new String[]{"9000.0"}, CompareFunction.EQUAL)
        ),
        LogicalOperator.AND
    );
    List<String> res = configJdbc.getDStore()
        .getDistinct(tProfile.getTableName(), cProfileCity, OrderBy.ASC, noMatch, 10, BEGIN, END);
    assertEquals(List.of(), res);
  }

  @Test
  public void distinctContainsWithOr() throws BeginEndWrongOrderException {
    CompositeFilter containsOr = new CompositeFilter(
        List.of(
            new FilterCondition(cProfileLastName, new String[]{"nov"}, CompareFunction.CONTAIN),
            new FilterCondition(cProfileLastName, new String[]{"PETR"}, CompareFunction.CONTAIN)
        ),
        LogicalOperator.OR
    );
    List<String> res = configJdbc.getDStore()
        .getDistinct(tProfile.getTableName(), cProfileLastName, OrderBy.ASC, containsOr, 10, BEGIN, END);
    assertEquals(List.of("Ivanov", "Novikova", "Petrov", "Smirnova"), res);
  }

  @Test
  public void ganttFilterBasic()
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    CompositeFilter filterLastName = new CompositeFilter(
        List.of(new FilterCondition(cProfileLastName, new String[]{"nov"}, CompareFunction.CONTAIN)),
        LogicalOperator.AND);

    List<GanttColumnCount> res = configJdbc.getDStore().getGanttCount(tProfile.getTableName(),
                                                                      cProfileLastName,
                                                                      cProfileCity,
                                                                      filterLastName, 1, 3);

    assertForGanttColumn(List.of(new GanttColumnCount("Ivanov", Map.of("Moscow", 2))), res);
  }

  @Test
  public void ganttFilterMultipleVariants()
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    CompositeFilter andFilter = new CompositeFilter(
        List.of(
            new FilterCondition(cProfileLastName, new String[]{"ov"}, CompareFunction.CONTAIN),
            new FilterCondition(cProfileCity, new String[]{"Moscow"}, CompareFunction.EQUAL)
        ),
        LogicalOperator.AND
    );

    List<GanttColumnCount> andResult = configJdbc.getDStore().getGanttCount(
        tProfile.getTableName(), cProfileLastName, cProfileCity, andFilter, 0, 10
    );

    List<GanttColumnCount> expectedAnd = List.of(
        new GanttColumnCount("Ivanov", Map.of("Moscow", 2)),
        new GanttColumnCount("Smirnova", Map.of("Moscow", 1))
    );
    assertForGanttColumn(expectedAnd, andResult);
  }

  @Test
  public void ganttFilterEdgeCases()
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    CompositeFilter emptyFilter = new CompositeFilter(List.of(), LogicalOperator.AND);
    List<GanttColumnCount> all = configJdbc.getDStore()
        .getGanttCount(tProfile.getTableName(), cProfileLastName, cProfileCity, emptyFilter, 1, 20);
    assertEquals(9, all.size());

    CompositeFilter none = new CompositeFilter(
        List.of(new FilterCondition(cProfileLastName, new String[]{"XYZ"}, CompareFunction.EQUAL)),
        LogicalOperator.AND);
    List<GanttColumnCount> noneRes = configJdbc.getDStore()
        .getGanttCount(tProfile.getTableName(), cProfileLastName, cProfileCity, none, 1, 10);
    assertTrue(noneRes.isEmpty());
  }

  @Test
  public void ganttFilterNumericCondition()
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    CompositeFilter filter = new CompositeFilter(
        List.of(new FilterCondition(cProfileSalary, new String[]{"3000.0"}, CompareFunction.EQUAL)),
        LogicalOperator.AND);

    List<GanttColumnCount> res = configJdbc.getDStore().getGanttCount(
        tProfile.getTableName(), cProfileLastName, cProfileSalary, filter, 1, 3);
    assertForGanttColumn(List.of(new GanttColumnCount("Petrov", Map.of("3000.0", 1))), res);
  }

  @Test
  public void ganttFilterDateCondition()
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    String formatted = "01.01.2023 01:01:01";
    CompositeFilter filter = new CompositeFilter(
        List.of(new FilterCondition(cProfileBirthday, new String[]{formatted}, CompareFunction.EQUAL)),
        LogicalOperator.AND);

    List<GanttColumnCount> res = configJdbc.getDStore().getGanttCount(
        tProfile.getTableName(), cProfileLastName, cProfileBirthday, filter, 1, 3);
    assertEquals(2, res.size());
  }

  @Test
  public void getStackedCountFilter() throws BeginEndWrongOrderException, SqlColMetadataException {
    GroupFunction groupFunction = GroupFunction.COUNT;
    long begin = 1;
    long end = 3;

    CompositeFilter moscowFilter = new CompositeFilter(
        List.of(new FilterCondition(cProfileCity, new String[]{"Moscow"}, CompareFunction.EQUAL)),
        LogicalOperator.AND);
    List<StackedColumn> actualMsk = getStackedFilter(cProfileLastName, groupFunction, moscowFilter, begin, end);
    assertResults(actualMsk, "Ivanov", 2);

    CompositeFilter ekbFilter = new CompositeFilter(
        List.of(new FilterCondition(cProfileCity, new String[]{"Yekaterinburg"}, CompareFunction.EQUAL)),
        LogicalOperator.AND);
    List<StackedColumn> actualEkb = getStackedFilter(cProfileLastName, groupFunction, ekbFilter, begin, end);
    assertResults(actualEkb, "Petrov", 1);

    CompositeFilter novFilter = new CompositeFilter(
        List.of(new FilterCondition(cProfileLastName, new String[]{"nov"}, CompareFunction.CONTAIN)),
        LogicalOperator.AND);
    List<StackedColumn> actualLastNameNov = getStackedFilter(cProfileLastName, groupFunction, novFilter, begin, end);

    CompositeFilter rovFilter = new CompositeFilter(
        List.of(new FilterCondition(cProfileLastName, new String[]{"rov"}, CompareFunction.CONTAIN)),
        LogicalOperator.AND);
    List<StackedColumn> actualLastNameRov = getStackedFilter(cProfileLastName, groupFunction, rovFilter, begin, end);

    assertResults(actualLastNameNov, "Ivanov", 2);
    assertResults(actualLastNameRov, "Petrov", 1);
  }

  @Test
  public void getStackedCountMultipleFilter() throws BeginEndWrongOrderException, SqlColMetadataException {
    GroupFunction groupFunction = GroupFunction.COUNT;
    long begin = 1;
    long end = 3;

    CompositeFilter moscowFilter = new CompositeFilter(
        List.of(new FilterCondition(cProfileCity, new String[]{"Moscow"}, CompareFunction.EQUAL)),
        LogicalOperator.AND);

    CompositeFilter multiConditionFilter = new CompositeFilter(
        List.of(
            new FilterCondition(cProfileCity, new String[]{"Moscow"}, CompareFunction.EQUAL),
            new FilterCondition(cProfileLastName, new String[]{"ov"}, CompareFunction.CONTAIN)
        ),
        LogicalOperator.AND);

    List<StackedColumn> actualMsk = getStackedFilter(cProfileLastName, groupFunction, moscowFilter, begin, end);
    List<StackedColumn> actualMulti = getStackedFilter(cProfileLastName, groupFunction, multiConditionFilter, begin, end);

    assertResults(actualMsk, "Ivanov", 2);
    assertResults(actualMulti, "Ivanov", 2);
  }

  @Test
  public void getStackedSumFilter() throws SQLException, BeginEndWrongOrderException, SqlColMetadataException {
    log.info("Use case: get stacked sum data with filter");
    GroupFunction groupFunction = GroupFunction.SUM;
    long begin = 1;
    long end = 3;

    CompositeFilter moscowFilter = new CompositeFilter(
        List.of(new FilterCondition(
            getCProfileByName("CITY"),
            new String[]{"Moscow"},
            CompareFunction.EQUAL)),
        LogicalOperator.AND);
    List<StackedColumn> actualMsk = getStackedFilter(tProfile, "HOUSE", groupFunction, moscowFilter, begin, end);
    assertEquals(3.0, actualMsk.get(0).getKeySum().get("HOUSE"));

    CompositeFilter ekbFilter = new CompositeFilter(
        List.of(new FilterCondition(
            getCProfileByName("CITY"),
            new String[]{"Yekaterinburg"},
            CompareFunction.EQUAL)),
        LogicalOperator.AND);
    List<StackedColumn> actualEkb = getStackedFilter(tProfile, "HOUSE", groupFunction, ekbFilter, begin, end);
    assertEquals(3.0, actualEkb.get(0).getKeySum().get("HOUSE"));

    CompositeFilter ovFilter = new CompositeFilter(
        List.of(new FilterCondition(
            getCProfileByName("LASTNAME"),
            new String[]{"ov"},
            CompareFunction.CONTAIN)),
        LogicalOperator.AND);
    List<StackedColumn> actualLastNameOv = getStackedFilter(tProfile, "HOUSE", groupFunction, ovFilter, begin, end);
    assertEquals(6.0, actualLastNameOv.get(0).getKeySum().get("HOUSE"));
  }

  @Test
  public void getStackedAvgFilter() throws SQLException, BeginEndWrongOrderException, SqlColMetadataException {
    log.info("Use case: get stacked avg data with filter");
    GroupFunction groupFunction = GroupFunction.AVG;
    long begin = 1;
    long end = 3;

    CompositeFilter moscowFilter = new CompositeFilter(
        List.of(new FilterCondition(
            getCProfileByName("CITY"),
            new String[]{"Moscow"},
            CompareFunction.EQUAL)),
        LogicalOperator.AND);
    List<StackedColumn> actualMsk = getStackedFilter(tProfile, "HOUSE", groupFunction, moscowFilter, begin, end);
    assertEquals(1.5, actualMsk.get(0).getKeyAvg().get("HOUSE"));

    CompositeFilter ekbFilter = new CompositeFilter(
        List.of(new FilterCondition(
            getCProfileByName("CITY"),
            new String[]{"Yekaterinburg"},
            CompareFunction.EQUAL)),
        LogicalOperator.AND);
    List<StackedColumn> actualEkb = getStackedFilter(tProfile, "HOUSE", groupFunction, ekbFilter, begin, end);
    assertEquals(3.0, actualEkb.get(0).getKeyAvg().get("HOUSE"));

    CompositeFilter ovFilter = new CompositeFilter(
        List.of(new FilterCondition(
            getCProfileByName("LASTNAME"),
            new String[]{"ov"},
            CompareFunction.CONTAIN)),
        LogicalOperator.AND);
    List<StackedColumn> actualLastNameOv = getStackedFilter(tProfile, "HOUSE", groupFunction, ovFilter, begin, end);
    assertEquals(2.0, actualLastNameOv.get(0).getKeyAvg().get("HOUSE"));
  }

  private List<StackedColumn> getStackedFilter(TProfile tProfile,
                                               String columnName,
                                               GroupFunction groupFunction,
                                               CompositeFilter compositeFilter,
                                               long begin,
                                               long end)
      throws SQLException, BeginEndWrongOrderException, SqlColMetadataException {

    putDataJdbc(configJdbc.getDStore(), tProfile, configJdbc.getH2DbConnection(), "SELECT * FROM manager");
    CProfile cProfile = getCProfileByName(columnName);
    return configJdbc.getDStore().getStacked(
        tProfile.getTableName(),
        cProfile,
        groupFunction,
        compositeFilter,
        begin,
        end);
  }

  @Test
  public void getGanttFilter() throws BeginEndWrongOrderException, SqlColMetadataException, GanttColumnNotSupportedException {
    CProfile firstGrpBy = cProfileLastName;
    CProfile secondGrpBy = cProfileCity;
    CProfile filterColumn = cProfileLastName;
    long begin = 1;
    long end = 3;

    CompositeFilter ovFilter = new CompositeFilter(
        List.of(new FilterCondition(filterColumn, new String[]{"ov"}, CompareFunction.CONTAIN)),
        LogicalOperator.AND
    );

    List<GanttColumnCount> actualOv = configJdbc.getDStore().getGanttCount(
        tProfile.getTableName(), firstGrpBy, secondGrpBy, ovFilter, 1, begin, end);

    List<GanttColumnCount> expectedOv = List.of(
        new GanttColumnCount("Petrov", Map.of("Yekaterinburg", 1)),
        new GanttColumnCount("Ivanov", Map.of("Moscow", 2))
    );
    assertForGanttColumn(expectedOv, actualOv);

    CompositeFilter novFilter = new CompositeFilter(
        List.of(new FilterCondition(filterColumn, new String[]{"nov"}, CompareFunction.CONTAIN)),
        LogicalOperator.AND
    );

    List<GanttColumnCount> actualNov = configJdbc.getDStore().getGanttCount(
        tProfile.getTableName(), firstGrpBy, secondGrpBy, novFilter, 1, begin, end);

    List<GanttColumnCount> expectedNov = List.of(
        new GanttColumnCount("Ivanov", Map.of("Moscow", 2))
    );
    assertForGanttColumn(expectedNov, actualNov);

    CompositeFilter exactFilter = new CompositeFilter(
        List.of(new FilterCondition(filterColumn, new String[]{"Ivanov"}, CompareFunction.EQUAL)),
        LogicalOperator.AND
    );

    List<GanttColumnCount> actualExact = configJdbc.getDStore().getGanttCount(
        tProfile.getTableName(), firstGrpBy, secondGrpBy, exactFilter, 1, begin, end);

    List<GanttColumnCount> expectedExact = List.of(
        new GanttColumnCount("Ivanov", Map.of("Moscow", 2))
    );
    assertForGanttColumn(expectedExact, actualExact);

    CompositeFilter multiFilter = new CompositeFilter(
        List.of(new FilterCondition(filterColumn, new String[]{"Ivanov", "Petrov"}, CompareFunction.EQUAL)),
        LogicalOperator.AND
    );

    List<GanttColumnCount> actualMulti = configJdbc.getDStore().getGanttCount(
        tProfile.getTableName(), firstGrpBy, secondGrpBy, multiFilter, 1, begin, end);

    List<GanttColumnCount> expectedMulti = List.of(
        new GanttColumnCount("Petrov", Map.of("Yekaterinburg", 1)),
        new GanttColumnCount("Ivanov", Map.of("Moscow", 2))
    );
    assertForGanttColumn(expectedMulti, actualMulti);

    CompositeFilter noMatchFilter = new CompositeFilter(
        List.of(new FilterCondition(filterColumn, new String[]{"xyz"}, CompareFunction.CONTAIN)),
        LogicalOperator.AND
    );

    List<GanttColumnCount> actualNoMatch = configJdbc.getDStore().getGanttCount(
        tProfile.getTableName(), firstGrpBy, secondGrpBy, noMatchFilter, 1, begin, end);
    assertTrue(actualNoMatch.isEmpty());

    CProfile cityFilterColumn = cProfileCity;
    CompositeFilter cityFilter = new CompositeFilter(
        List.of(new FilterCondition(cityFilterColumn, new String[]{"Moscow"}, CompareFunction.EQUAL)),
        LogicalOperator.AND
    );

    List<GanttColumnCount> actualCityFilter = configJdbc.getDStore().getGanttCount(
        tProfile.getTableName(), firstGrpBy, secondGrpBy, cityFilter, 1, begin, end);

    List<GanttColumnCount> expectedCityFilter = List.of(
        new GanttColumnCount("Ivanov", Map.of("Moscow", 2))
    );
    assertForGanttColumn(expectedCityFilter, actualCityFilter);

    CompositeFilter caseSensitiveFilter = new CompositeFilter(
        List.of(new FilterCondition(filterColumn, new String[]{"ivanov"}, CompareFunction.EQUAL)),
        LogicalOperator.AND
    );

    List<GanttColumnCount> actualCaseSensitive = configJdbc.getDStore().getGanttCount(
        tProfile.getTableName(), firstGrpBy, secondGrpBy, caseSensitiveFilter, 1, begin, end);
    assertTrue(actualCaseSensitive.isEmpty());
  }

  @Test
  public void getGanttSumFiltered() throws BeginEndWrongOrderException, SqlColMetadataException, GanttColumnNotSupportedException {
    CProfile lastNameGrpBy = cProfileLastName;
    CProfile houseSumColumn = cProfileSalary;
    CProfile firstNameFilter = cProfileFirstName;
    long begin = 1;
    long end = 3;

    CompositeFilter filterFirstNameEqual = new CompositeFilter(
        List.of(new FilterCondition(firstNameFilter, new String[]{"Ivan"}, CompareFunction.EQUAL)),
        LogicalOperator.AND);

    List<GanttColumnSum> actualIvan = configJdbc.getDStore().getGanttSum(
        tProfile.getTableName(),
        lastNameGrpBy,
        houseSumColumn,
        filterFirstNameEqual,
        begin,
        end);
    List<GanttColumnSum> expectedIvan = List.of(
        new GanttColumnSum("Ivanov", 2000.0)
    );
    assertGanttSumColumnsEqual(expectedIvan, actualIvan);

    CompositeFilter filterFirstNameContain = new CompositeFilter(
        List.of(new FilterCondition(firstNameFilter, new String[]{"e"}, CompareFunction.CONTAIN)),
        LogicalOperator.AND);

    List<GanttColumnSum> actualContainE = configJdbc.getDStore().getGanttSum(
        tProfile.getTableName(),
        lastNameGrpBy,
        houseSumColumn,
        filterFirstNameContain,
        begin,
        end);
    List<GanttColumnSum> expectedContainE = List.of(
        new GanttColumnSum("Ivanov", 1000.0),
        new GanttColumnSum("Petrov", 3000.0)
    );
    assertGanttSumColumnsEqual(expectedContainE, actualContainE);
  }

  @Test
  public void getDistinctWithFilter() throws BeginEndWrongOrderException {
    CompositeFilter compositeFilterMoscow = new CompositeFilter(
        List.of(new FilterCondition(cProfileCity, new String[]{"Moscow"}, CompareFunction.EQUAL)),
        LogicalOperator.AND);

    List<String> actualMoscow = configJdbc.getDStore().getDistinct(
        tProfile.getTableName(), cProfileFirstName, OrderBy.ASC, compositeFilterMoscow, 10, BEGIN, END);
    assertEquals(List.of("Alex", "Elena", "Ivan", "Maxim"), actualMoscow);

    CompositeFilter compositeFilterContainOv = new CompositeFilter(
        List.of(new FilterCondition(cProfileLastName, new String[]{"ov"}, CompareFunction.CONTAIN)),
        LogicalOperator.AND);

    List<String> actualContainOv = configJdbc.getDStore().getDistinct(
        tProfile.getTableName(), cProfileFirstName, OrderBy.ASC, compositeFilterContainOv, 10, BEGIN, END);
    assertEquals(List.of("Alex", "Anna", "Dmitry", "Elena", "Irina", "Ivan", "Oleg", "Olga", "Sergey"), actualContainOv);

    CompositeFilter compositeFilterLimit = new CompositeFilter(
        List.of(new FilterCondition(cProfileFirstName, new String[]{"Ivan"}, CompareFunction.EQUAL)),
        LogicalOperator.AND);

    List<String> actualLimit = configJdbc.getDStore().getDistinct(
        tProfile.getTableName(), cProfileLastName, OrderBy.ASC, compositeFilterLimit, 1, BEGIN, END);
    assertEquals(List.of("Ivanov"), actualLimit);

    CompositeFilter compositeFilterNoMatch = new CompositeFilter(
        List.of(new FilterCondition(cProfileCity, new String[]{"London"}, CompareFunction.EQUAL)),
        LogicalOperator.AND);

    List<String> actualNoMatch = configJdbc.getDStore().getDistinct(
        tProfile.getTableName(), cProfileFirstName, OrderBy.ASC, compositeFilterNoMatch, 10, BEGIN, END);
    assertTrue(actualNoMatch.isEmpty());

    CompositeFilter compositeFilterMulti = new CompositeFilter(
        List.of(new FilterCondition(cProfileFirstName, new String[]{"Alex", "Oleg"}, CompareFunction.EQUAL)),
        LogicalOperator.AND);

    List<String> actualMultiFilter = configJdbc.getDStore().getDistinct(
        tProfile.getTableName(), cProfileCity, OrderBy.DESC, compositeFilterMulti, 10, BEGIN, END);
    assertEquals(new HashSet<>(List.of("Yekaterinburg", "Moscow")), new HashSet<>(actualMultiFilter));
  }

  private List<StackedColumn> getStackedFilter(CProfile columnProfile,
                                               GroupFunction groupFunction,
                                               CompositeFilter compositeFilter,
                                               long begin,
                                               long end)
      throws BeginEndWrongOrderException, SqlColMetadataException {
    return configJdbc.getDStore().getStacked(
        tProfile.getTableName(),
        columnProfile,
        groupFunction,
        compositeFilter,
        begin,
        end);
  }

  private void assertResults(List<StackedColumn> actual, String firstValue, int expectedFirstValueCount) {
    assertNotNull(actual);
    assertEquals(1, actual.size());
    assertEquals(firstListStackedValue(actual, firstValue), expectedFirstValueCount);
  }

  private void assertResults(List<StackedColumn> actual, String firstValue, int expectedFirstValueCount,
                             String secondValue, int expectedSecondValueCount) {
    assertResults(actual, firstValue, expectedFirstValueCount);
    assertEquals(firstListStackedValue(actual, secondValue), expectedSecondValueCount);
  }

  private int firstListStackedValue(List<StackedColumn> list, String key) {
    return list.stream()
        .map(column -> column.getKeyCount().get(key))
        .filter(Objects::nonNull)
        .mapToInt(Integer::intValue)
        .sum();
  }

  private void assertGanttSumColumnsEqual(List<GanttColumnSum> expected, List<GanttColumnSum> actual) {
    assertEquals(expected.size(), actual.size(), "Number of result rows mismatch");
    Map<String, Double> expectedMap = expected.stream()
        .collect(Collectors.toMap(GanttColumnSum::getKey, GanttColumnSum::getValue));
    Map<String, Double> actualMap = actual.stream()
        .collect(Collectors.toMap(GanttColumnSum::getKey, GanttColumnSum::getValue));
    assertEquals(expectedMap, actualMap, "Result content mismatch");
  }

  private void assertForGanttColumn(List<GanttColumnCount> expected, List<GanttColumnCount> actual) {
    assertEquals(expected.size(), actual.size(), "Number of GanttColumnCount entries differs");
    Map<String, Map<String, Integer>> expectedMap = expected.stream()
        .collect(Collectors.toMap(GanttColumnCount::getKey, GanttColumnCount::getGantt));
    Map<String, Map<String, Integer>> actualMap = actual.stream()
        .collect(Collectors.toMap(GanttColumnCount::getKey, GanttColumnCount::getGantt));
    assertEquals(expectedMap.keySet(), actualMap.keySet(), "Keys don't match");
    expectedMap.forEach((key, expectedGantt) -> {
      Map<String, Integer> actualGantt = actualMap.get(key);
      assertEquals(expectedGantt, actualGantt, () -> "Gantt values differ for key: " + key);
    });
  }

  @AfterAll
  public void closeDb() {
    configJdbc.getBerkleyDB().closeDatabase();
  }
}