package ru.dimension.db.storage.mssql;

import com.sleepycat.persist.EntityCursor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.dbcp2.BasicDataSource;
import ru.dimension.db.core.metamodel.MetaModelApi;
import ru.dimension.db.model.CompareFunction;
import ru.dimension.db.model.GroupFunction;
import ru.dimension.db.model.OrderBy;
import ru.dimension.db.model.output.GanttColumnCount;
import ru.dimension.db.model.output.GanttColumnSum;
import ru.dimension.db.model.output.StackedColumn;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.cstype.CType;
import ru.dimension.db.sql.BatchResultSet;
import ru.dimension.db.storage.RawDAO;
import ru.dimension.db.storage.bdb.entity.Metadata;
import ru.dimension.db.storage.bdb.entity.MetadataKey;
import ru.dimension.db.storage.common.QueryJdbcApi;
import ru.dimension.db.storage.dialect.DatabaseDialect;
import ru.dimension.db.storage.dialect.MsSqlDialect;
import ru.dimension.db.util.CachedLastLinkedHashMap;

@Log4j2
public class RawMsSqlImpl extends QueryJdbcApi implements RawDAO {

  private final MetaModelApi metaModelApi;
  private final DatabaseDialect databaseDialect;

  private final Map<Byte, Metadata> primaryIndex;

  public RawMsSqlImpl(MetaModelApi metaModelApi,
                      BasicDataSource basicDataSource) {
    super(basicDataSource);

    this.metaModelApi = metaModelApi;

    this.databaseDialect = new MsSqlDialect();
    this.primaryIndex = new HashMap<>();
  }

  @Override
  public void putMetadata(byte tableId,
                          long blockId,
                          byte[] rawCTypeKeys,
                          int[] rawColIds,
                          int[] enumColIds,
                          int[] histogramColIds) {
    MetadataKey metadataKey = new MetadataKey(tableId, blockId);
    this.primaryIndex.put(tableId, new Metadata(metadataKey, rawCTypeKeys, rawColIds, enumColIds, histogramColIds));
  }

  @Override
  public void putByte(byte tableId,
                      long blockId,
                      int[] mapping,
                      byte[][] data) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public void putInt(byte tableId,
                     long blockId,
                     int[] mapping,
                     int[][] data) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public void putLong(byte tableId,
                      long blockId,
                      int[] mapping,
                      long[][] data) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public void putFloat(byte tableId,
                       long blockId,
                       int[] mapping,
                       float[][] data) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public void putDouble(byte tableId,
                        long blockId,
                        int[] mapping,
                        double[][] data) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public void putString(byte tableId,
                        long blockId,
                        int[] mapping,
                        String[][] data) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public void putEnum(byte tableId,
                      long blockId,
                      int[] mapping,
                      byte[][] data) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public void putCompressed(byte tableId,
                            long blockId,
                            List<Integer> rawDataTimeStampMapping,
                            List<List<Long>> rawDataTimestamp,
                            List<Integer> rawDataIntMapping,
                            List<List<Integer>> rawDataInt,
                            List<Integer> rawDataLongMapping,
                            List<List<Long>> rawDataLong,
                            List<Integer> rawDataFloatMapping,
                            List<List<Float>> rawDataFloat,
                            List<Integer> rawDataDoubleMapping,
                            List<List<Double>> rawDataDouble,
                            List<Integer> rawDataStringMapping,
                            List<List<String>> rawDataString) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public void putCompressed(byte tableId,
                            long blockId,
                            CachedLastLinkedHashMap<Integer, Integer> rawDataTimeStampMapping,
                            List<List<Long>> rawDataTimestamp,
                            CachedLastLinkedHashMap<Integer, Integer> rawDataIntMapping,
                            List<List<Integer>> rawDataInt,
                            CachedLastLinkedHashMap<Integer, Integer> rawDataLongMapping,
                            List<List<Long>> rawDataLong,
                            CachedLastLinkedHashMap<Integer, Integer> rawDataFloatMapping,
                            List<List<Float>> rawDataFloat,
                            CachedLastLinkedHashMap<Integer, Integer> rawDataDoubleMapping,
                            List<List<Double>> rawDataDouble,
                            CachedLastLinkedHashMap<Integer, Integer> rawDataStringMapping,
                            List<List<String>> rawDataString) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public void putCompressed(byte tableId,
                            long blockId,
                            CachedLastLinkedHashMap<Integer, Integer> rawDataTimeStampMapping,
                            List<List<Long>> rawDataTimestamp) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public void putCompressed(byte tableId,
                            long blockId,
                            Map<CType, Map<Integer, List<Object>>> rawDataByType) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public byte[] getRawByte(byte tableId,
                           long blockId,
                           int colId) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public int[] getRawInt(byte tableId,
                         long blockId,
                         int colId) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public long[] getRawLong(byte tableId,
                           long blockId,
                           int colId) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public float[] getRawFloat(byte tableId,
                             long blockId,
                             int colId) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public double[] getRawDouble(byte tableId,
                               long blockId,
                               int colId) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public String[] getRawString(byte tableId,
                               long blockId,
                               int colId) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public List<Long> getListBlockIds(byte tableId,
                                    long begin,
                                    long end) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public EntityCursor<Metadata> getMetadataEntityCursor(MetadataKey begin,
                                                        MetadataKey end) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public Metadata getMetadata(MetadataKey metadataKey) {
    Metadata metadata = primaryIndex.get(metadataKey.getTableId());

    if (metadata == null) {
      log.info("No data found for metadata key -> " + metadataKey);
      return new Metadata();
    }

    return metadata;
  }

  @Override
  public long getPreviousBlockId(byte tableId,
                                 long blockId) {
    return getLastBlockIdLocal(tableId, 0L, blockId);
  }

  @Override
  public long getFirstBlockId(byte tableId,
                              int tsColId,
                              long begin,
                              long end) {
    return getFirstBlockIdLocal(tableId, begin, end);
  }

  @Override
  public long getLastBlockId(byte tableId) {
    return getLastBlockIdLocal(tableId, 0L, Long.MAX_VALUE);
  }

  @Override
  public long getLastBlockId(byte tableId,
                             int tsColId,
                             long begin,
                             long end) {
    return getLastBlockIdLocal(tableId, begin, end);
  }

  @Override
  public List<StackedColumn> getStacked(String tableName,
                                        CProfile tsCProfile,
                                        CProfile cProfile,
                                        GroupFunction groupFunction,
                                        CProfile cProfileFilter,
                                        String[] filterData,
                                        CompareFunction compareFunction,
                                        long begin,
                                        long end) {
    return getStackedCommon(tableName, tsCProfile, cProfile, groupFunction, cProfileFilter, filterData, compareFunction, begin, end, databaseDialect);
  }

  @Override
  public List<GanttColumnCount> getGantt(String tableName,
                                         CProfile tsCProfile,
                                         CProfile firstGrpBy,
                                         CProfile secondGrpBy,
                                         long begin,
                                         long end) {
    return getGantt(tableName, tsCProfile, firstGrpBy, secondGrpBy, null, null, null, begin, end, databaseDialect);
  }

  @Override
  public List<GanttColumnCount> getGantt(String tableName,
                                         CProfile tsCProfile,
                                         CProfile firstGrpBy,
                                         CProfile secondGrpBy,
                                         CProfile cProfileFilter,
                                         String[] filterData,
                                         CompareFunction compareFunction,
                                         long begin,
                                         long end) {
    return getGantt(tableName, tsCProfile, firstGrpBy, secondGrpBy, cProfileFilter, filterData, compareFunction, begin, end, databaseDialect);
  }

  @Override
  public List<GanttColumnSum> getGanttSum(String tableName,
                                          CProfile tsCProfile,
                                          CProfile firstGrpBy,
                                          CProfile secondGrpBy,
                                          long begin,
                                          long end) {
    return getGanttSum(tableName, tsCProfile, firstGrpBy, secondGrpBy, begin, end, databaseDialect);
  }

  @Override
  public List<GanttColumnSum> getGanttSum(String tableName,
                                          CProfile tsCProfile,
                                          CProfile firstGrpBy,
                                          CProfile secondGrpBy,
                                          CProfile cProfileFilter,
                                          String[] filterData,
                                          CompareFunction compareFunction,
                                          long begin,
                                          long end) {
    return getGanttSumWithFilter(tableName, tsCProfile, firstGrpBy, secondGrpBy, cProfileFilter, filterData, compareFunction, begin, end, databaseDialect);
  }

  @Override
  public List<String> getDistinct(String tableName,
                                  CProfile tsCProfile,
                                  CProfile cProfile,
                                  OrderBy orderBy,
                                  int limit,
                                  long begin,
                                  long end) {
    checkDataType(cProfile, "BINARY");

    return getDistinctCommon(tableName, tsCProfile, cProfile, orderBy, limit, begin, end, databaseDialect);
  }

  @Override
  public List<String> getDistinct(String tableName,
                                  CProfile tsCProfile,
                                  CProfile cProfile,
                                  OrderBy orderBy,
                                  int limit,
                                  long begin,
                                  long end,
                                  CProfile cProfileFilter,
                                  String[] filterData,
                                  CompareFunction compareFunction) {
    checkDataType(cProfile, "BINARY");

    return getDistinctWithFilterCommon(tableName, tsCProfile, cProfile, orderBy, limit, begin, end,
                                       cProfileFilter, filterData, compareFunction, databaseDialect);
  }

  @Override
  public BatchResultSet getBatchResultSet(String tableName,
                                          long begin,
                                          long end,
                                          int fetchSize,
                                          List<CProfile> cProfiles) {
    return getBatchResultSetCommon(tableName, begin, end, fetchSize, cProfiles, databaseDialect);
  }

  private long getFirstBlockIdLocal(byte tableId,
                                    long begin,
                                    long end) {
    String tableName = metaModelApi.getTableName(tableId);
    CProfile tsCProfile = metaModelApi.getTimestampCProfile(tableName);

    return getFirstBlockIdLocal(tableName, tsCProfile, begin, end, databaseDialect);
  }

  private long getLastBlockIdLocal(byte tableId,
                                   long begin,
                                   long end) {
    String tableName = metaModelApi.getTableName(tableId);
    CProfile tsCProfile = metaModelApi.getTimestampCProfile(tableName);

    return getLastBlockIdLocal(tableName, tsCProfile, begin, end, databaseDialect);
  }
}