package ru.dimension.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.extern.log4j.Log4j2;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;
import ru.dimension.db.backend.BerkleyDB;
import ru.dimension.db.common.mode.DirectMode;
import ru.dimension.db.common.mode.JDBCMode;
import ru.dimension.db.config.DBaseConfig;
import ru.dimension.db.exception.EnumByteExceedException;
import ru.dimension.db.exception.SqlColMetadataException;
import ru.dimension.db.exception.TableNameEmptyException;
import ru.dimension.db.model.BdbAndDBase;
import ru.dimension.db.model.CompareFunction;
import ru.dimension.db.model.DBaseTestConfig;
import ru.dimension.db.model.GroupFunction;
import ru.dimension.db.model.LoadDataType;
import ru.dimension.db.model.OrderBy;
import ru.dimension.db.model.PermutationState;
import ru.dimension.db.model.Person;
import ru.dimension.db.model.filter.CompositeFilter;
import ru.dimension.db.model.filter.FilterCondition;
import ru.dimension.db.model.filter.LogicalOperator;
import ru.dimension.db.model.output.BlockKeyTail;
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
import ru.dimension.db.sql.BatchResultSet;

/**
 * This runner executes the **entire** use-case test battery that was originally located
 * in DBaseUseCasesCodeTest, but now runs under different parameter permutations.
 */
@Log4j2
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Disabled
public class DBaseRunnerUseCasesTest implements DirectMode, JDBCMode {

  private H2Database h2Db;

  private BerkleyDB berkleyDB;

  @TempDir
  static File databaseDir;

  @BeforeAll
  void globalSetUp() throws SQLException {
    h2Db = new H2Database("jdbc:h2:mem:dbrunner");
    loadDataTablePersonH2(h2Db);
  }

  @AfterAll
  void globalTearDown() throws SQLException {
    h2Db.close();
    berkleyDB.closeDatabase();
  }

  private List<Map<String, Object>> generateTestConfigurations(PermutationState state) {
    return switch (state) {
      case NONE  -> List.of(Map.of(
          "compression", false,
          "batchSize", 10,
          "csTypeMap", Map.of(),
          "indexType", IType.GLOBAL,
          "analyzeType", AType.ON_LOAD,
          "loadDataType", LoadDataType.JDBC
      ));
      case PARTIAL -> List.of(
          Map.of(
              "compression", true,
              "batchSize", 5,
              "csTypeMap", Map.of(),
              "indexType", IType.GLOBAL,
              "analyzeType", AType.ON_LOAD,
              "loadDataType", LoadDataType.JDBC
          ),
          Map.of(
              "compression", false,
              "batchSize", 15,
              "csTypeMap", Map.of(),
              "indexType", IType.LOCAL,
              "analyzeType", AType.ON_LOAD,
              "loadDataType", LoadDataType.JDBC
          ));
      case ALL -> List.of(
          Map.of(
              "compression", true,
              "batchSize", 5,
              "csTypeMap", Map.of(),
              "indexType", IType.GLOBAL,
              "analyzeType", AType.ON_LOAD,
              "loadDataType", LoadDataType.JDBC
          ),
          Map.of(
              "compression", false,
              "batchSize", 10,
              "csTypeMap", Map.of(),
              "indexType", IType.LOCAL,
              "analyzeType", AType.ON_LOAD,
              "loadDataType", LoadDataType.JDBC
          ),
          Map.of(
              "compression", true,
              "batchSize", 15,
              "csTypeMap", Map.of(),
              "indexType", IType.GLOBAL,
              "analyzeType", AType.ON_LOAD,
              "loadDataType", LoadDataType.JDBC
          ));
    };
  }

  @Order(4)
  @Test
  void testUseCases() {
    testUseCasesInParallel(PermutationState.NONE);
  }

  @Order(5)
  @Test
  void testUseCasesPartial() {
    testUseCasesInParallel(PermutationState.PARTIAL);
  }

  @Order(6)
  @Test
  void testUseCasesAll() {
    testUseCasesInParallel(PermutationState.ALL);
  }

  void testUseCasesInParallel(PermutationState permutationState) {
    AtomicInteger counter = new AtomicInteger();
    generateTestConfigurations(permutationState).forEach(cfg -> {
      try {
        log.info("### Running use-cases with config: {}", cfg);
        counter.incrementAndGet();

        BdbAndDBase bdb = getBdbAndDBaseUnique();
        DBaseTestConfig testCfg = createTestConfig();
        runAllUseCaseTests(testCfg, cfg);

        testCfg.getDStore().closeBackendDb();
        testCfg.getBerkleyDB().closeDatabase();
        testCfg.getBerkleyDB().removeDirectory();

        bdb.getBerkleyDB().closeDatabase();
        bdb.getBerkleyDB().removeDirectory();
      } catch (Exception ex) {
        throw new RuntimeException("Use-case run failed for " + cfg, ex);
      }
    });
    log.info("### Finished {} configurations", counter.get());
  }

  private DBaseTestConfig createTestConfig() throws IOException {
    File databaseDirDirect = createSubDirWithPostfix(databaseDir, "direct");

    return new DBaseTestConfig(databaseDirDirect, h2Db);
  }

  private static File createSubDirWithPostfix(File rootDir, String subDirName) {
    File subDir = new File(rootDir, subDirName);
    if (!subDir.mkdir()) {
      throw new RuntimeException("Failed to create subdirectory: " + subDirName);
    }
    return subDir;
  }

  void runAllUseCaseTests(DBaseTestConfig testCfg, Map<String, Object> cfg) throws Exception {
    testGetTProfile(testCfg);
    testLoadDirectTableMetadata(testCfg);
    testLoadJdbcTableMetadata(testCfg);
    testSetTimestampColumn(testCfg);
    testPutDataDirect(testCfg);
    testPutDataJdbcBatch(testCfg);
    testPutDataJdbc(testCfg);
    testGetBlockKeyTailList(testCfg);
    testGetStacked(testCfg);
    testGetStackedCountFilter(testCfg);
    testGetStackedCountMultipleFilter(testCfg);
    testGetStackedSumFilter(testCfg);
    testGetStackedAvgFilter(testCfg);
    testGetGantt(testCfg);
    testGetGanttFilter(testCfg);
    testGetGanttSum(testCfg);
    testGetDistinct(testCfg);
    testGetDistinctWithFilter(testCfg);
    testGetRawDataAll(testCfg);
    testGetRawDataAllFilter(testCfg);
    testGetRawDataByColumn(testCfg);
    testGetBatchResultSetTimeSeriesTable(testCfg);
    testGetFirst(testCfg);
    testGetLast(testCfg);
    testGetGanttSumFiltered(testCfg);
  }

  private void testGetTProfile(DBaseTestConfig cfg) throws TableNameEmptyException {
    String table = "uc_get_tprofile";
    loadMetadataDirect(cfg, table, TType.REGULAR, IType.GLOBAL, AType.ON_LOAD, BType.BERKLEYDB, true);
    assertEquals(table, cfg.getDStore().getTProfile(table).getTableName());
  }

  private void testLoadDirectTableMetadata(DBaseTestConfig cfg) throws TableNameEmptyException {
    String table = "uc_load_direct_md";
    SProfile sp = getSProfileDirect(table, TType.TIME_SERIES, IType.GLOBAL, AType.ON_LOAD, BType.BERKLEYDB, true);
    assertEquals(table, cfg.getDStore().loadDirectTableMetadata(sp).getTableName());
  }

  private void testLoadJdbcTableMetadata(DBaseTestConfig cfg) throws SQLException, TableNameEmptyException {
    String table = "uc_load_jdbc_md";
    SProfile sp = getSProfileJdbc(table, TType.REGULAR, IType.GLOBAL, AType.ON_LOAD, BType.BERKLEYDB, true);
    sp.getCsTypeMap().put("ID", CSType.builder().isTimeStamp(true).sType(SType.RAW).cType(CType.LONG).build());
    assertEquals(table, cfg.getDStore().loadJdbcTableMetadata(cfg.getH2DbConnection(), "SELECT * FROM person", sp).getTableName());
  }

  private void testSetTimestampColumn(DBaseTestConfig cfg) throws TableNameEmptyException {
    String table = "uc_set_ts_col";
    loadMetadataDirect(cfg, table, TType.REGULAR, IType.GLOBAL, AType.ON_LOAD, BType.BERKLEYDB, true);
    TProfile tp = cfg.getDStore().getTProfile(table);
    CProfile before = tp.getCProfiles().stream().filter(c -> c.getCsType().isTimeStamp()).findFirst().orElseThrow();
    CProfile after = getCProfileByName(tp, "LONG_FIELD");
    cfg.getDStore().setTimestampColumn(table, after.getColName());
    assertFalse(before.getCsType().isTimeStamp());
    assertTrue(after.getCsType().isTimeStamp());
  }

  private void testPutDataDirect(DBaseTestConfig cfg) throws TableNameEmptyException, EnumByteExceedException, SqlColMetadataException {
    String table = "uc_put_direct";
    TProfile tp = setupDirectProfile(table, cfg);
    cfg.getDStore().putDataDirect(table, prepareData());
    assertData(cfg, table, List.of(List.of(1707387748310L, 17073877482L, 17073877482D, "a31bce67-d1ab-485b-9ffa-850385e298ac")));
  }

  private void testPutDataJdbcBatch(DBaseTestConfig cfg) throws SQLException, TableNameEmptyException {
    String table = "uc_put_jdbc_batch";
    TProfile tp = setupJdbcProfile(table, cfg);
    putDataJdbc(cfg.getDStore(), tp, cfg.getH2DbConnection(), "SELECT * FROM person");
    assertRawDataJdbc(cfg, table, Long.MIN_VALUE, Long.MAX_VALUE);
  }

  private void testPutDataJdbc(DBaseTestConfig cfg) throws SQLException, TableNameEmptyException, EnumByteExceedException, SqlColMetadataException {
    String table = "uc_put_jdbc";
    TProfile tp = setupJdbcProfile(table, cfg);
    putDataJdbcBatch(cfg.getDStore(), tp, cfg.getH2DbConnection(), "SELECT * FROM person");
    assertRawDataJdbc(cfg, table, Long.MIN_VALUE, Long.MAX_VALUE);
  }

  private void testGetBlockKeyTailList(DBaseTestConfig cfg) throws Exception {
    String table = "uc_block_key_tail";
    TProfile tp = setupJdbcProfile(table, cfg);
    putDataJdbcBatch(cfg.getDStore(), tp, cfg.getH2DbConnection(), "SELECT * FROM person");
    List<BlockKeyTail> tails = cfg.getDStore().getBlockKeyTailList(table, 1, 3);
    Assertions.assertEquals(1, tails.size());
    Assertions.assertEquals(1, tails.get(0).getKey());
    Assertions.assertEquals(3, tails.get(0).getTail());
  }

  private void testGetStacked(DBaseTestConfig cfg) throws Exception {
    String table = "uc_stacked";
    TProfile tp = setupJdbcProfile(table, cfg);
    putDataJdbcBatch(cfg.getDStore(), tp, cfg.getH2DbConnection(), "SELECT * FROM person");
    List<StackedColumn> res = cfg.getDStore().getStacked(table, getCProfileByName(tp, "LASTNAME"), GroupFunction.COUNT, null, 1, 3);
    assertResults(res, "Ivanov", 2);
    assertResults(res, "Petrov", 1);
  }

  private void testGetStackedCountFilter(DBaseTestConfig cfg) throws Exception {
    String table = "uc_stacked_filter";
    TProfile tp = setupJdbcProfile(table, cfg);
    putDataJdbcBatch(cfg.getDStore(), tp, cfg.getH2DbConnection(), "SELECT * FROM person");
    CompositeFilter moscow = new CompositeFilter(
        List.of(new FilterCondition(getCProfileByName(tp, "CITY"), new String[]{"Moscow"}, CompareFunction.EQUAL)),
        LogicalOperator.AND);
    List<StackedColumn> res = cfg.getDStore().getStacked(table, getCProfileByName(tp, "LASTNAME"), GroupFunction.COUNT, moscow, 1, 3);
    assertResults(res, "Ivanov", 2);
  }

  private void testGetStackedCountMultipleFilter(DBaseTestConfig cfg) throws Exception {
    String table = "uc_stacked_multifilter";
    TProfile tp = setupJdbcProfile(table, cfg);
    putDataJdbcBatch(cfg.getDStore(), tp, cfg.getH2DbConnection(), "SELECT * FROM person");
    CompositeFilter f = new CompositeFilter(
        List.of(
            new FilterCondition(getCProfileByName(tp, "CITY"), new String[]{"Moscow"}, CompareFunction.EQUAL),
            new FilterCondition(getCProfileByName(tp, "LASTNAME"), new String[]{"ov"}, CompareFunction.CONTAIN)
        ),
        LogicalOperator.AND);
    List<StackedColumn> res = cfg.getDStore().getStacked(table, getCProfileByName(tp, "LASTNAME"), GroupFunction.COUNT, f, 1, 3);
    assertResults(res, "Ivanov", 2);
  }

  private void testGetStackedSumFilter(DBaseTestConfig cfg) throws Exception {
    String table = "uc_stacked_sum";
    TProfile tp = setupJdbcProfile(table, cfg);
    putDataJdbcBatch(cfg.getDStore(), tp, cfg.getH2DbConnection(), "SELECT * FROM person");
    CompositeFilter f = new CompositeFilter(
        List.of(new FilterCondition(getCProfileByName(tp, "CITY"), new String[]{"Moscow"}, CompareFunction.EQUAL)),
        LogicalOperator.AND);
    List<StackedColumn> res = cfg.getDStore().getStacked(table, getCProfileByName(tp, "HOUSE"), GroupFunction.SUM, f, 1, 3);
    assertEquals(3.0, res.get(0).getKeySum().get("HOUSE"));
  }

  private void testGetStackedAvgFilter(DBaseTestConfig cfg) throws Exception {
    String table = "uc_stacked_avg";
    TProfile tp = setupJdbcProfile(table, cfg);
    putDataJdbcBatch(cfg.getDStore(), tp, cfg.getH2DbConnection(), "SELECT * FROM person");
    CompositeFilter f = new CompositeFilter(
        List.of(new FilterCondition(getCProfileByName(tp, "CITY"), new String[]{"Moscow"}, CompareFunction.EQUAL)),
        LogicalOperator.AND);
    List<StackedColumn> res = cfg.getDStore().getStacked(table, getCProfileByName(tp, "HOUSE"), GroupFunction.AVG, f, 1, 3);
    assertEquals(1.5, res.get(0).getKeyAvg().get("HOUSE"));
  }

  private void testGetGantt(DBaseTestConfig cfg) throws Exception {
    String table = "uc_gantt";
    TProfile tp = setupJdbcProfile(table, cfg);
    putDataJdbcBatch(cfg.getDStore(), tp, cfg.getH2DbConnection(), "SELECT * FROM person");
    List<GanttColumnCount> res = cfg.getDStore().getGanttCount(table,
                                                               getCProfileByName(tp, "LASTNAME"),
                                                               getCProfileByName(tp, "CITY"),
                                                               null, 1, 3);
    assertForGantt(List.of(new GanttColumnCount("Petrov", Map.of("Yekaterinburg", 1)),
                           new GanttColumnCount("Ivanov", Map.of("Moscow", 2))), res);
  }

  private void testGetGanttFilter(DBaseTestConfig cfg) throws Exception {
    String table = "uc_gantt_filter";
    TProfile tp = setupJdbcProfile(table, cfg);
    putDataJdbcBatch(cfg.getDStore(), tp, cfg.getH2DbConnection(), "SELECT * FROM person");

    CompositeFilter filterLastName = new CompositeFilter(
        List.of(new FilterCondition(getCProfileByName(tp, "LASTNAME"), new String[]{"nov"}, CompareFunction.CONTAIN)),
        LogicalOperator.AND);

    List<GanttColumnCount> res = cfg.getDStore().getGanttCount(table,
                                                               getCProfileByName(tp, "LASTNAME"),
                                                               getCProfileByName(tp, "CITY"),
                                                               filterLastName, 1, 3);
    assertForGantt(List.of(new GanttColumnCount("Ivanov", Map.of("Moscow", 2))), res);
  }

  private void testGetGanttSum(DBaseTestConfig cfg) throws Exception {
    String table = "uc_gantt_sum";
    TProfile tp = setupJdbcProfile(table, cfg);
    putDataJdbcBatch(cfg.getDStore(), tp, cfg.getH2DbConnection(), "SELECT * FROM person");
    List<GanttColumnSum> res = cfg.getDStore().getGanttSum(table,
                                                           getCProfileByName(tp, "LASTNAME"),
                                                           getCProfileByName(tp, "HOUSE"),
                                                           null, 1, 3);
    assertGanttSumColumnsEqual(List.of(new GanttColumnSum("Petrov", 3.0),
                                       new GanttColumnSum("Ivanov", 3.0)), res);
  }

  private void testGetDistinct(DBaseTestConfig cfg) throws Exception {
    String table = "uc_distinct";
    TProfile tp = setupJdbcProfile(table, cfg);
    putDataJdbcBatch(cfg.getDStore(), tp, cfg.getH2DbConnection(), "SELECT * FROM person");
    List<String> res = cfg.getDStore().getDistinct(table, getCProfileByName(tp, "FIRSTNAME"), OrderBy.ASC, null, 10, 1, 3);
    assertEquals(List.of("Alex", "Ivan", "Oleg"), res);
  }

  private void testGetDistinctWithFilter(DBaseTestConfig cfg) throws Exception {
    String table = "uc_distinct_filter";
    TProfile tp = setupJdbcProfile(table, cfg);
    putDataJdbcBatch(cfg.getDStore(), tp, cfg.getH2DbConnection(), "SELECT * FROM person");

    CompositeFilter compositeFilter = new CompositeFilter(
        List.of(new FilterCondition(getCProfileByName(tp, "CITY"), new String[]{"Moscow"}, CompareFunction.EQUAL)),
        LogicalOperator.AND);

    List<String> res = cfg.getDStore().getDistinct(table,
                                                   getCProfileByName(tp, "FIRSTNAME"),
                                                   OrderBy.ASC, compositeFilter, 10, 1, 3);
    assertEquals(List.of("Alex", "Ivan"), res);
  }

  private void testGetRawDataAll(DBaseTestConfig cfg) throws Exception {
    String table = "uc_raw_all";
    TProfile tp = setupJdbcProfile(table, cfg);
    putDataJdbcBatch(cfg.getDStore(), tp, cfg.getH2DbConnection(), "SELECT * FROM person");
    assertEquals(3, cfg.getDStore().getRawDataAll(table, 1, 3).size());
  }

  private void testGetRawDataAllFilter(DBaseTestConfig cfg) throws Exception {
    String table = "uc_raw_all_filter";
    TProfile tp = setupJdbcProfile(table, cfg);
    putDataJdbcBatch(cfg.getDStore(), tp, cfg.getH2DbConnection(), "SELECT * FROM person");
    List<List<Object>> res = cfg.getDStore().getRawDataAll(table, getCProfileByName(tp, "FIRSTNAME"), "Alex", 1, 3);
    assertEquals(1, res.size());
    assertEquals("Alex", String.valueOf(res.get(0).get(1)));
  }

  private void testGetRawDataByColumn(DBaseTestConfig cfg) throws Exception {
    String table = "uc_raw_col";
    TProfile tp = setupJdbcProfile(table, cfg);
    putDataJdbcBatch(cfg.getDStore(), tp, cfg.getH2DbConnection(), "SELECT * FROM person");
    List<List<Object>> res = cfg.getDStore().getRawDataByColumn(table, getCProfileByName(tp, "FIRSTNAME"), 1, 3);
    assertEquals(3, res.size());
  }

  private void testGetBatchResultSetTimeSeriesTable(DBaseTestConfig cfg) throws Exception {
    String table = "uc_batch_ts";
    TProfile tp = setupJdbcProfile(table, cfg);
    putDataJdbcBatch(cfg.getDStore(), tp, cfg.getH2DbConnection(), "SELECT * FROM person");

    BatchResultSet brs = null;
    try {
      brs = cfg.getDStore().getBatchResultSet(table, 1, 3, 10);
      int rows = 0;
      while (brs.next()) rows += brs.getObject().size();
      assertEquals(3, rows);
    } finally {

    }
  }

  private void testGetFirst(DBaseTestConfig cfg) throws Exception {
    String table = "uc_first";
    TProfile tp = setupJdbcProfile(table, cfg);
    putDataJdbcBatch(cfg.getDStore(), tp, cfg.getH2DbConnection(), "SELECT * FROM person");
    assertEquals(1L, cfg.getDStore().getFirst(table, Long.MIN_VALUE, Long.MAX_VALUE));
  }

  private void testGetLast(DBaseTestConfig cfg) throws Exception {
    String table = "uc_last";
    TProfile tp = setupJdbcProfile(table, cfg);
    putDataJdbcBatch(cfg.getDStore(), tp, cfg.getH2DbConnection(), "SELECT * FROM person");
    assertEquals(3L, cfg.getDStore().getLast(table, Long.MIN_VALUE, Long.MAX_VALUE));
  }

  private void testGetGanttSumFiltered(DBaseTestConfig cfg) throws Exception {
    String table = "uc_gantt_sum_filtered";
    TProfile tp = setupJdbcProfile(table, cfg);
    putDataJdbcBatch(cfg.getDStore(), tp, cfg.getH2DbConnection(), "SELECT * FROM person");

    CompositeFilter compositeFilter = new CompositeFilter(
        List.of(new FilterCondition(getCProfileByName(tp, "FIRSTNAME"),
                                    new String[]{"Ivan"}, CompareFunction.EQUAL)),
        LogicalOperator.AND);

    List<GanttColumnSum> res = cfg.getDStore().getGanttSum(table,
                                                           getCProfileByName(tp, "LASTNAME"),
                                                           getCProfileByName(tp, "HOUSE"),
                                                           compositeFilter, 1, 3);
    assertGanttSumColumnsEqual(List.of(new GanttColumnSum("Ivanov", 2.0)), res);
  }

  private TProfile setupJdbcProfile(String tableName, DBaseTestConfig cfg) throws SQLException, TableNameEmptyException {
    SProfile sp = getSProfileJdbc(tableName, TType.TIME_SERIES, IType.LOCAL, AType.ON_LOAD, BType.BERKLEYDB, true);
    sp.getCsTypeMap().put("ID", CSType.builder().isTimeStamp(true).sType(SType.RAW).cType(CType.LONG).build());
    return cfg.getDStore().loadJdbcTableMetadata(cfg.getH2DbConnection(), "SELECT * FROM person", sp);
  }

  private void loadDataTablePersonH2(H2Database h2Db) throws SQLException {
    String table = "PERSON";
    h2Db.execute("""
            CREATE TABLE PERSON (
              id INT,
              firstname VARCHAR(64),
              lastname VARCHAR(64),
              house INT,
              city VARCHAR(64),
              birthday TIMESTAMP
            )
            """);
    LocalDateTime d = LocalDateTime.of(2023, 1, 1, 1, 1, 1);
    h2Db.insert(Person.builder().id(1).firstname("Alex").lastname("Ivanov").house(1).city("Moscow").birthday(d).build());
    h2Db.insert(Person.builder().id(2).firstname("Ivan").lastname("Ivanov").house(2).city("Moscow").birthday(d).build());
    h2Db.insert(Person.builder().id(3).firstname("Oleg").lastname("Petrov").house(3).city("Yekaterinburg").birthday(d).build());
  }

  private CProfile getCProfileByName(TProfile tp, String name) {
    return tp.getCProfiles().stream().filter(c -> c.getColName().equals(name)).findFirst().orElseThrow();
  }

  private void assertData(DBaseTestConfig cfg, String table, List<List<Object>> expected) {
    List<List<Object>> actual = cfg.getDStore().getRawDataAll(table, Long.MIN_VALUE, Long.MAX_VALUE);
    assertEquals(expected.toString(), actual.toString());
  }

  private void assertRawDataJdbc(DBaseTestConfig cfg, String table, long begin, long end) {
    List<List<Object>> actual = cfg.getDStore().getRawDataAll(table, begin, end);
    assertEquals(3, actual.size());
  }

  private void assertResults(List<StackedColumn> act, String key, int count) {
    assertNotNull(act);
    assertEquals(count, act.stream()
        .map(c -> c.getKeyCount().get(key))
        .filter(Objects::nonNull)
        .mapToInt(Integer::intValue)
        .sum());
  }

  private void assertForGantt(List<GanttColumnCount> expected, List<GanttColumnCount> actual) {
    assertEquals(expected.size(), actual.size());
    expected.forEach(e -> assertEquals(e.getGantt(),
                                       actual.stream().filter(a -> a.getKey().equals(e.getKey())).findFirst().orElseThrow().getGantt()));
  }

  private void assertGanttSumColumnsEqual(List<GanttColumnSum> expected, List<GanttColumnSum> actual) {
    assertEquals(expected.size(), actual.size());
    Map<String, Double> exp = expected.stream().collect(Collectors.toMap(GanttColumnSum::getKey, GanttColumnSum::getValue));
    Map<String, Double> act = actual.stream().collect(Collectors.toMap(GanttColumnSum::getKey, GanttColumnSum::getValue));
    assertEquals(exp, act);
  }

  protected BdbAndDBase getBdbAndDBaseUnique() throws IOException {
    Path path = createUniqueTempDir();
    String directory = path.toAbsolutePath().normalize().toFile().getAbsolutePath();

    berkleyDB = new BerkleyDB(directory, true);

    DBaseConfig dBaseConfig = new DBaseConfig().setConfigDirectory(directory);
    DBase dBase = new DBase(dBaseConfig, berkleyDB.getStore());

    return new BdbAndDBase(dBase.getDStore(), berkleyDB);
  }

  public static Path createUniqueTempDir() throws IOException {
    return Files.createTempDirectory("junit_dimension_db_" + UUID.randomUUID());
  }
}