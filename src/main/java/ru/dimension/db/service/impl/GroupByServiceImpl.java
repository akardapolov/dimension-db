package ru.dimension.db.service.impl;

import com.sleepycat.persist.EntityCursor;
import java.text.Collator;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import ru.dimension.db.core.metamodel.MetaModelApi;
import ru.dimension.db.exception.BeginEndWrongOrderException;
import ru.dimension.db.exception.SqlColMetadataException;
import ru.dimension.db.metadata.DataType;
import ru.dimension.db.model.OrderBy;
import ru.dimension.db.model.filter.CompositeFilter;
import ru.dimension.db.model.filter.FilterCondition;
import ru.dimension.db.model.output.GanttColumnCount;
import ru.dimension.db.model.output.GanttColumnSum;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.cstype.CType;
import ru.dimension.db.model.profile.table.BType;
import ru.dimension.db.service.CommonServiceApi;
import ru.dimension.db.service.GroupByService;
import ru.dimension.db.storage.Converter;
import ru.dimension.db.storage.EnumDAO;
import ru.dimension.db.storage.HistogramDAO;
import ru.dimension.db.storage.RawDAO;
import ru.dimension.db.storage.bdb.entity.Metadata;
import ru.dimension.db.storage.bdb.entity.MetadataKey;

@Log4j2
public class GroupByServiceImpl extends CommonServiceApi implements GroupByService {

  private final MetaModelApi metaModelApi;
  private final Converter converter;
  private final HistogramDAO histogramDAO;
  private final RawDAO rawDAO;
  private final EnumDAO enumDAO;

  public GroupByServiceImpl(MetaModelApi metaModelApi,
                            Converter converter,
                            HistogramDAO histogramDAO,
                            RawDAO rawDAO,
                            EnumDAO enumDAO) {
    this.metaModelApi = metaModelApi;
    this.converter = converter;
    this.histogramDAO = histogramDAO;
    this.rawDAO = rawDAO;
    this.enumDAO = enumDAO;
  }

  public static List<GanttColumnCount> mergeGanttColumnsByKey(List<GanttTask> tasks) {
    Map<String, GanttColumnCount> resultMap = new HashMap<>();
    for (GanttTask task : tasks) {
      List<GanttColumnCount> list = task.getGanttColumnCountList();
      if (list != null) {
        for (GanttColumnCount col : list) {
          GanttColumnCount existing =
              resultMap.computeIfAbsent(col.getKey(), k -> new GanttColumnCount(k, new HashMap<>()));
          col.getGantt().forEach((subKey, cnt) ->
                                     existing.getGantt().merge(subKey, cnt, Integer::sum));
        }
      }
    }
    return new ArrayList<>(resultMap.values());
  }

  @Override
  public List<GanttColumnCount> getGanttCount(String tableName,
                                              CProfile firstGrpBy,
                                              CProfile secondGrpBy,
                                              CompositeFilter compositeFilter,
                                              long begin,
                                              long end) throws SqlColMetadataException {
    BType bType = metaModelApi.getBackendType(tableName);
    if (!BType.BERKLEYDB.equals(bType)) {
      CProfile tsProfile = metaModelApi.getTimestampCProfile(tableName);
      return rawDAO.getGanttCount(tableName, tsProfile, firstGrpBy, secondGrpBy, compositeFilter, begin, end);
    }
    return getListGanttColumnIndexLocal(tableName, firstGrpBy, secondGrpBy, compositeFilter, begin, end);
  }

  private List<GanttColumnCount> getListGanttColumnIndexLocal(String tableName,
                                                              CProfile firstGrpBy,
                                                              CProfile secondGrpBy,
                                                              CompositeFilter compositeFilter,
                                                              long begin,
                                                              long end) {
    byte tableId = metaModelApi.getTableId(tableName);
    int tsColId = metaModelApi.getTimestampCProfile(tableName).getColId();

    long previousBlockId = this.rawDAO.getPreviousBlockId(tableId, begin);
    Map.Entry<MetadataKey, MetadataKey> keyEntry = getMetadataKeyPair(tableId, begin, end, previousBlockId);

    Map<String, Map<String, Integer>> mapFinal = new HashMap<>();

    try (EntityCursor<Metadata> cursor = rawDAO.getMetadataEntityCursor(keyEntry.getKey(), keyEntry.getValue())) {
      Metadata columnKey;
      while ((columnKey = cursor.next()) != null) {
        long blockId = columnKey.getMetadataKey().getBlockId();
        long[] timestamps = rawDAO.getRawLong(tableId, blockId, tsColId);

        // Get filter values for all conditions in the composite filter
        Map<FilterCondition, String[]> filterConditionValuesMap = new HashMap<>();
        if (compositeFilter != null) {
          for (FilterCondition condition : compositeFilter.getConditions()) {
            String[] conditionValues = getStringArrayValues(rawDAO, enumDAO, histogramDAO, converter,
                                                            tableId, condition.getCProfile(), blockId, timestamps);
            filterConditionValuesMap.put(condition, conditionValues);
          }
        }

        // Get group by column values
        String[] firstValues = getStringArrayValues(rawDAO, enumDAO, histogramDAO, converter,
                                                    tableId, firstGrpBy, blockId, timestamps);
        String[] secondValues = getStringArrayValues(rawDAO, enumDAO, histogramDAO, converter,
                                                     tableId, secondGrpBy, blockId, timestamps);

        // Process each row
        for (int i = 0; i < timestamps.length; i++) {
          if (timestamps[i] >= begin && timestamps[i] <= end) {
            // Check composite filter conditions
            boolean includeRow = true;
            if (compositeFilter != null && !compositeFilter.getConditions().isEmpty()) {
              String[] filterValues = new String[compositeFilter.getConditions().size()];
              int idx = 0;
              for (FilterCondition condition : compositeFilter.getConditions()) {
                String rawValue = filterConditionValuesMap.get(condition)[i];
                String formattedValue = formatFloatingPoint(rawValue, condition.getCProfile().getCsType().getCType());
                filterValues[idx++] = formattedValue;
              }
              includeRow = compositeFilter.test(filterValues);
            }

            if (includeRow) {
              String firstValue = firstValues[i];
              String secondValue = secondValues[i];
              mapFinal.computeIfAbsent(firstValue, k -> new HashMap<>())
                  .merge(secondValue, 1, Integer::sum);
            }
          }
        }
      }
    } catch (Exception e) {
      log.error("Error processing gantt data with composite filter", e);
    }

    // Handle complex data types
    if (DataType.ARRAY.equals(firstGrpBy.getCsType().getDType())
        || DataType.ARRAY.equals(secondGrpBy.getCsType().getDType())) {
      mapFinal = handleArray(firstGrpBy, secondGrpBy, mapFinal);
    }
    if (DataType.MAP.equals(firstGrpBy.getCsType().getDType())
        || DataType.MAP.equals(secondGrpBy.getCsType().getDType())) {
      return handleMap(firstGrpBy, secondGrpBy, mapFinal);
    }

    return mapFinal.entrySet().stream()
        .map(e -> GanttColumnCount.builder()
            .key(e.getKey())
            .gantt(e.getValue())
            .build())
        .collect(Collectors.toList());
  }

  @Override
  public List<GanttColumnCount> getGanttCount(String tableName,
                                              CProfile firstGrpBy,
                                              CProfile secondGrpBy,
                                              CompositeFilter compositeFilter,
                                              int batchSize,
                                              long begin,
                                              long end) throws SqlColMetadataException, BeginEndWrongOrderException {
    if (firstGrpBy.getCsType().isTimeStamp() | secondGrpBy.getCsType().isTimeStamp()) {
      throw new SqlColMetadataException("Group by not supported for timestamp column..");
    }
    if (begin > end) {
      throw new BeginEndWrongOrderException("Begin value must be less the end one..");
    }
    log.info("First column profile: " + firstGrpBy);
    log.info("Second column profile: " + secondGrpBy);

    List<Map.Entry<Long, Long>> ranges = new ArrayList<>();
    long totalRange = end - begin + 1;
    long batchTimestampSize = totalRange / batchSize;
    for (int i = 0; i < batchSize; i++) {
      long batchStart = begin + i * batchTimestampSize;
      long batchEnd = (i == batchSize - 1) ? end : batchStart + batchTimestampSize - 1;
      log.info("Processing batch " + toLocalDateTime(batchStart) + " --- " + toLocalDateTime(batchEnd));
      ranges.add(Map.entry(batchStart, batchEnd));
    }

    ForkJoinPool pool = new ForkJoinPool();
    List<GanttTask> tasks = new ArrayList<>();
    for (Map.Entry<Long, Long> entry : ranges) {
      tasks.add(new GanttTask(tableName, firstGrpBy, secondGrpBy, compositeFilter, entry.getKey(), entry.getValue()));
    }
    for (GanttTask task : tasks) {
      pool.execute(task);
    }
    pool.shutdown();
    try {
      pool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.error("Interrupted while waiting for task completion", e);
    }
    return mergeGanttColumnsByKey(tasks);
  }

  @Override
  public List<GanttColumnSum> getGanttSum(String tableName,
                                          CProfile firstGrpBy,
                                          CProfile secondGrpBy,
                                          CompositeFilter compositeFilter,
                                          long begin,
                                          long end) throws SqlColMetadataException {
    CProfile tsProfile = metaModelApi.getTimestampCProfile(tableName);
    if (!tsProfile.getCsType().isTimeStamp()) {
      throw new SqlColMetadataException("Timestamp column not defined");
    }
    if (CType.STRING == secondGrpBy.getCsType().getCType()) {
      throw new SqlColMetadataException("String data to compute SUM function not supported for column: " + secondGrpBy.getColName());
    }
    if (secondGrpBy.getCsType().isTimeStamp()) {
      throw new SqlColMetadataException("Not supported for timestamp column");
    }

    BType bType = metaModelApi.getBackendType(tableName);
    if (!BType.BERKLEYDB.equals(bType)) {
      return rawDAO.getGanttSum(tableName, tsProfile, firstGrpBy, secondGrpBy, compositeFilter, begin, end);
    }

    byte tableId = metaModelApi.getTableId(tableName);
    Map<String, Double> resultMap = new HashMap<>();

    long previousBlockId = this.rawDAO.getPreviousBlockId(tableId, begin);
    Map.Entry<MetadataKey, MetadataKey> keyEntry = getMetadataKeyPair(tableId, begin, end, previousBlockId);

    try (EntityCursor<Metadata> cursor = rawDAO.getMetadataEntityCursor(keyEntry.getKey(), keyEntry.getValue())) {
      Metadata columnKey;
      while ((columnKey = cursor.next()) != null) {
        long blockId = columnKey.getMetadataKey().getBlockId();
        long[] timestamps = rawDAO.getRawLong(tableId, blockId, tsProfile.getColId());

        // Get filter values for all conditions in the composite filter
        Map<FilterCondition, String[]> filterConditionValuesMap = new HashMap<>();
        if (compositeFilter != null) {
          for (FilterCondition condition : compositeFilter.getConditions()) {
            String[] conditionValues = getStringArrayValues(rawDAO, enumDAO, histogramDAO, converter,
                                                            tableId, condition.getCProfile(), blockId, timestamps);
            filterConditionValuesMap.put(condition, conditionValues);
          }
        }

        // Get group by column values and sum column values
        String[] firstColumnValues = getStringArrayValues(rawDAO, enumDAO, histogramDAO, converter,
                                                          tableId, firstGrpBy, blockId, timestamps);
        double[] secondColumnValues = getDoubleArrayValues(rawDAO, enumDAO, histogramDAO, converter,
                                                           tableId, secondGrpBy, blockId, timestamps);

        for (int i = 0; i < timestamps.length; i++) {
          if (timestamps[i] >= begin && timestamps[i] <= end) {
            // Check composite filter conditions
            boolean includeRow = true;
            if (compositeFilter != null && !compositeFilter.getConditions().isEmpty()) {
              String[] filterValues = new String[compositeFilter.getConditions().size()];
              int idx = 0;
              for (FilterCondition condition : compositeFilter.getConditions()) {
                String rawValue = filterConditionValuesMap.get(condition)[i];
                String formattedValue = formatFloatingPoint(rawValue, condition.getCProfile().getCsType().getCType());
                filterValues[idx++] = formattedValue;
              }
              includeRow = compositeFilter.test(filterValues);
            }

            if (includeRow) {
              try {
                String groupKey = firstColumnValues[i];
                double value = secondColumnValues[i];
                resultMap.merge(groupKey, value, Double::sum);
              } catch (NumberFormatException e) {
                log.warn("Skipping non-numeric value in sum column: {}", secondColumnValues[i]);
              }
            }
          }
        }
      }
    } catch (Exception e) {
      log.error("Error processing filtered gantt sum data with composite filter", e);
      throw new SqlColMetadataException("Error processing filtered gantt sum data: " + e.getMessage());
    }

    return resultMap.entrySet().stream()
        .map(entry -> new GanttColumnSum(entry.getKey(), entry.getValue()))
        .collect(Collectors.toList());
  }

  @Override
  public List<String> getDistinct(String tableName,
                                  CProfile cProfile,
                                  OrderBy orderBy,
                                  CompositeFilter compositeFilter,
                                  int limit,
                                  long begin,
                                  long end) {
    BType bType = metaModelApi.getBackendType(tableName);
    CProfile tsProfile = metaModelApi.getTimestampCProfile(tableName);
    if (tsProfile.getColId() == cProfile.getColId()) {
      throw new RuntimeException("Not supported for timestamp column");
    }
    if (!BType.BERKLEYDB.equals(bType)) {
      return rawDAO.getDistinct(tableName, tsProfile, cProfile, orderBy, compositeFilter, limit, begin, end);
    }

    Set<String> columnData = new LinkedHashSet<>();
    byte tableId = metaModelApi.getTableId(tableName);
    long previousBlockId = this.rawDAO.getPreviousBlockId(tableId, begin);
    Map.Entry<MetadataKey, MetadataKey> keyEntry = getMetadataKeyPair(tableId, begin, end, previousBlockId);

    try (EntityCursor<Metadata> cursor = rawDAO.getMetadataEntityCursor(keyEntry.getKey(), keyEntry.getValue())) {
      Metadata columnKey;
      while ((columnKey = cursor.next()) != null) {
        long blockId = columnKey.getMetadataKey().getBlockId();
        long[] timestamps = rawDAO.getRawLong(tableId, blockId, tsProfile.getColId());

        // Pre-store all filter condition values in a map for condition-based access
        Map<FilterCondition, String[]> filterConditionValuesMap = new HashMap<>();
        if (compositeFilter != null && !compositeFilter.getConditions().isEmpty()) {
          for (FilterCondition condition : compositeFilter.getConditions()) {
            String[] conditionValues = getStringArrayValues(rawDAO, enumDAO, histogramDAO, converter,
                                                            tableId, condition.getCProfile(), blockId, timestamps);
            filterConditionValuesMap.put(condition, conditionValues);
          }
        }

        // Get the distinct column values
        String[] distinctValues = getStringArrayValues(rawDAO, enumDAO, histogramDAO, converter,
                                                       tableId, cProfile, blockId, timestamps);

        for (int i = 0; i < timestamps.length; i++) {
          if (timestamps[i] >= begin && timestamps[i] <= end) {
            // Check composite filter conditions
            boolean includeRow = true;
            if (compositeFilter != null && !compositeFilter.getConditions().isEmpty()) {
              String[] filterValues = new String[compositeFilter.getConditions().size()];
              int idx = 0;
              for (FilterCondition condition : compositeFilter.getConditions()) {
                String rawValue = filterConditionValuesMap.get(condition)[i];
                String formattedValue = formatFloatingPoint(rawValue, condition.getCProfile().getCsType().getCType());
                filterValues[idx++] = formattedValue;
              }
              includeRow = compositeFilter.test(filterValues);
            }

            if (includeRow) {
              String value = distinctValues[i];
              value = formatFloatingPoint(value, cProfile.getCsType().getCType());
              columnData.add(value);
            }
          }
        }
      }
    } catch (Exception e) {
      log.error("Error processing distinct data with composite filter", e);
    }

    List<String> resultList = new ArrayList<>(columnData);

    if (cProfile.getCsType().getCType() == CType.FLOAT ||
        cProfile.getCsType().getCType() == CType.DOUBLE) {
      resultList.sort(Comparator.comparingDouble(Double::parseDouble));
    } else if (cProfile.getCsType().getCType() == CType.STRING) {
      resultList.sort(Collator.getInstance());
    } else {
      Collections.sort(resultList);
    }

    if (OrderBy.DESC.equals(orderBy)) {
      Collections.reverse(resultList);
    }

    return resultList.subList(0, Math.min(limit, resultList.size()));
  }

  private String formatFloatingPoint(String value, CType cType) {
    if (cType != CType.FLOAT && cType != CType.DOUBLE) {
      return value;
    }
    try {
      double num = Double.parseDouble(value);
      return String.format("%.1f", num).replace(",", ".");
    } catch (NumberFormatException e) {
      return value;
    }
  }

  class GanttTask extends RecursiveAction {
    private final String tableName;
    private final CProfile firstGrpBy;
    private final CProfile secondGrpBy;
    private final CompositeFilter compositeFilter;
    private final long begin;
    private final long end;
    @Getter
    private List<GanttColumnCount> ganttColumnCountList;

    GanttTask(String tableName,
              CProfile firstGrpBy,
              CProfile secondGrpBy,
              CompositeFilter compositeFilter,
              long begin,
              long end) {
      this.tableName = tableName;
      this.firstGrpBy = firstGrpBy;
      this.secondGrpBy = secondGrpBy;
      this.compositeFilter = compositeFilter;
      this.begin = begin;
      this.end = end;
    }
    @Override
    protected void compute() {
      log.info("Start task at: " + LocalDateTime.now());
      try {
        ganttColumnCountList = getGanttCount(tableName, firstGrpBy, secondGrpBy, compositeFilter, begin, end);
      } catch (SqlColMetadataException e) {
        throw new RuntimeException(e);
      }
      log.info("End task at: " + LocalDateTime.now());
    }
  }
}