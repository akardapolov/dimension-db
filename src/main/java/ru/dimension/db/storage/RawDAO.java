package ru.dimension.db.storage;

import com.sleepycat.persist.EntityCursor;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import ru.dimension.db.model.CompareFunction;
import ru.dimension.db.model.GroupFunction;
import ru.dimension.db.model.OrderBy;
import ru.dimension.db.model.output.GanttColumnCount;
import ru.dimension.db.model.output.GanttColumnSum;
import ru.dimension.db.model.output.StackedColumn;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.cstype.CType;
import ru.dimension.db.sql.BatchResultSet;
import ru.dimension.db.storage.bdb.entity.Metadata;
import ru.dimension.db.storage.bdb.entity.MetadataKey;
import ru.dimension.db.util.CachedLastLinkedHashMap;

public interface RawDAO {

  void putMetadata(byte tableId,
                   long blockId,
                   byte[] rawCTypeKeys,
                   int[] rawColIds,
                   int[] enumColIds,
                   int[] histogramColIds);

  void putByte(byte tableId,
               long blockId,
               int[] mapping,
               byte[][] data);

  void putInt(byte tableId,
              long blockId,
              int[] mapping,
              int[][] data);

  void putLong(byte tableId,
               long blockId,
               int[] mapping,
               long[][] data);

  void putFloat(byte tableId,
                long blockId,
                int[] mapping,
                float[][] data);

  void putDouble(byte tableId,
                 long blockId,
                 int[] mapping,
                 double[][] data);

  void putString(byte tableId,
                 long blockId,
                 int[] mapping,
                 String[][] data);

  void putEnum(byte tableId,
               long blockId,
               int[] mapping,
               byte[][] data);

  void putCompressed(byte tableId,
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
                     List<List<String>> rawDataString)
      throws IOException;

  void putCompressed(byte tableId,
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
                     List<List<String>> rawDataString);

  void putCompressed(byte tableId,
                     long blockId,
                     CachedLastLinkedHashMap<Integer, Integer> rawDataTimeStampMapping,
                     List<List<Long>> rawDataTimestamp);

  void putCompressed(byte tableId, long blockId, Map<CType, Map<Integer, List<Object>>> rawDataByType);

  byte[] getRawByte(byte tableId,
                    long blockId,
                    int colId);

  int[] getRawInt(byte tableId,
                  long blockId,
                  int colId);

  float[] getRawFloat(byte tableId,
                      long blockId,
                      int colId);

  long[] getRawLong(byte tableId,
                    long blockId,
                    int colId);

  double[] getRawDouble(byte tableId,
                        long blockId,
                        int colId);

  String[] getRawString(byte tableId,
                        long blockId,
                        int colId);

  List<Long> getListBlockIds(byte tableId,
                             long begin,
                             long end);

  EntityCursor<Metadata> getMetadataEntityCursor(MetadataKey begin,
                                                 MetadataKey end);

  Metadata getMetadata(MetadataKey metadataKey);

  long getPreviousBlockId(byte tableId,
                          long blockId);

  long getFirstBlockId(byte tableId,
                       int tsColId,
                       long begin,
                       long end);

  long getLastBlockId(byte tableId);

  long getLastBlockId(byte tableId,
                      int tsColId,
                      long begin,
                      long end);

  List<StackedColumn> getStacked(String tableName,
                                 CProfile tsCProfile,
                                 CProfile cProfile,
                                 GroupFunction groupFunction,
                                 CProfile cProfileFilter,
                                 String[] filterData,
                                 CompareFunction compareFunction,
                                 long begin,
                                 long end);

  List<GanttColumnCount> getGantt(String tableName,
                                  CProfile tsCProfile,
                                  CProfile firstGrpBy,
                                  CProfile secondGrpBy,
                                  long begin,
                                  long end);

  List<GanttColumnCount> getGantt(String tableName,
                                  CProfile tsCProfile,
                                  CProfile firstGrpBy,
                                  CProfile secondGrpBy,
                                  CProfile cProfileFilter,
                                  String[] filterData,
                                  CompareFunction compareFunction,
                                  long begin,
                                  long end);

  List<GanttColumnSum> getGanttSum(String tableName,
                                   CProfile tsCProfile,
                                   CProfile firstGrpBy,
                                   CProfile secondGrpBy,
                                   long begin,
                                   long end);

  List<GanttColumnSum> getGanttSum(String tableName,
                                   CProfile tsCProfile,
                                   CProfile firstGrpBy,
                                   CProfile secondGrpBy,
                                   CProfile cProfileFilter,
                                   String[] filterData,
                                   CompareFunction compareFunction,
                                   long begin,
                                   long end);

  List<String> getDistinct(String tableName,
                           CProfile tsCProfile,
                           CProfile cProfile,
                           OrderBy orderBy,
                           int limit,
                           long begin,
                           long end);

  List<String> getDistinct(String tableName,
                           CProfile tsCProfile,
                           CProfile cProfile,
                           OrderBy orderBy,
                           int limit,
                           long begin,
                           long end,
                           CProfile cProfileFilter,
                           String[] filterData,
                           CompareFunction compareFunction);

  BatchResultSet getBatchResultSet(String tableName,
                                   long begin,
                                   long end,
                                   int fetchSize,
                                   List<CProfile> cProfiles);
}