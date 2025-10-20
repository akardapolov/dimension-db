package ru.dimension.db.service.impl;

import static ru.dimension.db.util.MapArrayUtil.parseStringToTypedArray;
import static ru.dimension.db.util.MapArrayUtil.parseStringToTypedMap;

import com.sleepycat.persist.EntityCursor;
import java.text.Collator;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import ru.dimension.db.core.metamodel.MetaModelApi;
import ru.dimension.db.exception.BeginEndWrongOrderException;
import ru.dimension.db.exception.SqlColMetadataException;
import ru.dimension.db.metadata.DataType;
import ru.dimension.db.model.GroupFunction;
import ru.dimension.db.model.OrderBy;
import ru.dimension.db.model.filter.CompositeFilter;
import ru.dimension.db.model.filter.FilterCondition;
import ru.dimension.db.model.output.GanttColumnCount;
import ru.dimension.db.model.output.GanttColumnSum;
import ru.dimension.db.model.output.StackedColumn;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.cstype.CType;
import ru.dimension.db.model.profile.cstype.SType;
import ru.dimension.db.model.profile.table.BType;
import ru.dimension.db.service.CommonServiceApi;
import ru.dimension.db.service.GroupByService;
import ru.dimension.db.service.mapping.Mapper;
import ru.dimension.db.storage.Converter;
import ru.dimension.db.storage.EnumDAO;
import ru.dimension.db.storage.HistogramDAO;
import ru.dimension.db.storage.RawDAO;
import ru.dimension.db.storage.bdb.entity.Metadata;
import ru.dimension.db.storage.bdb.entity.MetadataKey;
import ru.dimension.db.storage.format.StorageContext;
import ru.dimension.db.storage.format.StorageManager;

@Log4j2
public class GroupByServiceImpl extends CommonServiceApi implements GroupByService {

  private final MetaModelApi metaModelApi;
  private final Converter converter;
  private final HistogramDAO histogramDAO;
  private final RawDAO rawDAO;
  private final EnumDAO enumDAO;

  private final StorageManager storageManager;

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
    this.storageManager = new StorageManager();
  }

  @Override
  public List<StackedColumn> getStacked(String tableName,
                                        CProfile cProfile,
                                        GroupFunction groupFunction,
                                        CompositeFilter compositeFilter,
                                        long begin,
                                        long end) throws SqlColMetadataException {
    CProfile tsProfile = metaModelApi.getTimestampCProfile(tableName);

    if (!tsProfile.getCsType().isTimeStamp()) {
      throw new SqlColMetadataException("Timestamp column not defined..");
    }

    if (cProfile.getCsType().isTimeStamp()) {
      throw new SqlColMetadataException("Not supported for timestamp column..");
    }

    if (groupFunction == GroupFunction.SUM || groupFunction == GroupFunction.AVG) {
      CType cType = cProfile.getCsType().getCType();
      if (!(cType == CType.INT || cType == CType.LONG || cType == CType.FLOAT || cType == CType.DOUBLE)) {
        throw new RuntimeException("Group function " + groupFunction + " not supported for non-numeric column: " + cProfile.getColName());
      }
    }

    BType bType = metaModelApi.getBackendType(tableName);
    if (!BType.BERKLEYDB.equals(bType)) {
      return rawDAO.getStacked(tableName, tsProfile, cProfile, groupFunction, compositeFilter, begin, end);
    }

    byte tableId = metaModelApi.getTableId(tableName);
    int tsColId = tsProfile.getColId();

    long previousBlockId = this.rawDAO.getPreviousBlockId(tableId, begin);
    Map.Entry<MetadataKey, MetadataKey> keyEntry = getMetadataKeyPair(tableId, begin, end, previousBlockId);

    List<StackedColumn> result = new ArrayList<>();

    boolean aggregateAsSingle = (groupFunction == GroupFunction.SUM || groupFunction == GroupFunction.AVG);
    double totalSum = 0.0;
    int totalCount = 0;
    boolean anyAccepted = false;

    try (EntityCursor<Metadata> cursor = rawDAO.getMetadataEntityCursor(keyEntry.getKey(), keyEntry.getValue())) {
      Metadata columnKey;

      while ((columnKey = cursor.next()) != null) {
        long blockId = columnKey.getMetadataKey().getBlockId();
        long[] timestamps = rawDAO.getRawLong(tableId, blockId, tsColId);

        StorageContext context = StorageContext.builder()
            .tableId(tableId)
            .blockId(blockId)
            .timestamps(timestamps)
            .rawDAO(rawDAO)
            .enumDAO(enumDAO)
            .histogramDAO(histogramDAO)
            .converter(converter)
            .build();

        SType targetSType = getSType(cProfile.getColId(), columnKey);

        Map<FilterCondition, String[]> filterCache = buildFilterCacheWithFormats(compositeFilter, context, columnKey);
        BitSet acceptedRows = acceptedRows(timestamps, begin, end, compositeFilter, filterCache);

        if (acceptedRows.isEmpty()) {
          continue;
        }

        switch (groupFunction) {
          case COUNT: {
            processStackedCount(context, cProfile, targetSType, acceptedRows, blockId, timestamps, result, compositeFilter);
            break;
          }
          case SUM:
          case AVG: {
            try {
              double[] targetValues = storageManager.readDoubleValues(context, cProfile, targetSType);
              for (int i = acceptedRows.nextSetBit(0); i >= 0 && i < targetValues.length; i = acceptedRows.nextSetBit(i + 1)) {
                totalSum += targetValues[i];
                totalCount++;
              }
              anyAccepted = true;
            } catch (Exception e) {
              log.error("Failed to fetch numeric values for '{}' using SType '{}': {}",
                        cProfile.getColName(), targetSType, e.toString(), e);
              throw new RuntimeException("Failed to compute " + groupFunction + " for " + cProfile.getColName(), e);
            }
            break;
          }
          default:
            throw new RuntimeException("Group function not supported: " + groupFunction);
        }
      }
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
      log.error("Error processing stacked data: ", e);
    }

    if (aggregateAsSingle && anyAccepted) {
      StackedColumn column = StackedColumn.builder()
          .key(begin)
          .tail(end)
          .build();
      if (groupFunction == GroupFunction.SUM) {
        column.setKeySum(new HashMap<>());
        column.getKeySum().put(cProfile.getColName(), totalSum);
      } else {
        double average = (totalCount > 0) ? (totalSum / totalCount) : 0.0;
        column.setKeyAvg(new HashMap<>());
        column.getKeyAvg().put(cProfile.getColName(), average);
      }
      result.add(column);
    }

    return result;
  }

  private void processStackedCount(StorageContext context,
                                   CProfile cProfile,
                                   SType sType,
                                   BitSet acceptedRows,
                                   long blockId,
                                   long[] timestamps,
                                   List<StackedColumn> result,
                                   CompositeFilter compositeFilter) {
    try {
      String[] targetValues = storageManager.readStringValues(context, cProfile, sType);
      Map<String, Integer> valueCounts = new LinkedHashMap<>();

      for (int i = acceptedRows.nextSetBit(0); i >= 0 && i < targetValues.length; i = acceptedRows.nextSetBit(i + 1)) {
        String value = targetValues[i] != null ? targetValues[i] : "";
        processValueByDataType(value, valueCounts, cProfile, compositeFilter);
      }

      if (!valueCounts.isEmpty()) {
        long lastTimestamp = timestamps.length > 0 ? timestamps[timestamps.length - 1] : blockId;
        StackedColumn column = StackedColumn.builder()
            .key(blockId)
            .tail(lastTimestamp)
            .keyCount(valueCounts)
            .build();
        result.add(column);
      }
    } catch (Exception e) {
      log.error("Failed to process stacked count for column '{}': {}", cProfile.getColName(), e.toString(), e);
    }
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

        StorageContext context = StorageContext.builder()
            .tableId(tableId)
            .blockId(blockId)
            .timestamps(timestamps)
            .rawDAO(rawDAO)
            .enumDAO(enumDAO)
            .histogramDAO(histogramDAO)
            .converter(converter)
            .build();

        SType firstSType = getSType(firstGrpBy.getColId(), columnKey);
        SType secondSType = getSType(secondGrpBy.getColId(), columnKey);

        Map<FilterCondition, String[]> cache = buildFilterCacheWithFormats(compositeFilter, context, columnKey);
        BitSet acceptedRows = acceptedRows(timestamps, begin, end, compositeFilter, cache);

        if (acceptedRows.isEmpty()) {
          continue;
        }

        processDualColumnsWithFilter(context,
                                     firstGrpBy, secondGrpBy,
                                     firstSType, secondSType,
                                     acceptedRows, mapFinal);
      }

    } catch (Exception e) {
      log.error(e.getMessage());
    }

    if (DataType.ARRAY.equals(firstGrpBy.getCsType().getDType()) || DataType.ARRAY.equals(secondGrpBy.getCsType().getDType())) {
      Map<String, Map<String, Integer>> map = handleArray(firstGrpBy, secondGrpBy, mapFinal, compositeFilter);
      mapFinal.clear();
      mapFinal.putAll(map);
    }

    if (DataType.MAP.equals(firstGrpBy.getCsType().getDType()) || DataType.MAP.equals(secondGrpBy.getCsType().getDType())) {
      return handleMap(firstGrpBy, secondGrpBy, mapFinal, compositeFilter);
    }

    List<GanttColumnCount> list = new ArrayList<>();
    mapFinal.forEach((key, val) -> list.add(GanttColumnCount.builder().key(key).gantt(val).build()));
    return list;
  }

  private void processDualColumnsWithFilter(StorageContext context,
                                            CProfile firstGrpBy,
                                            CProfile secondGrpBy,
                                            SType firstSType,
                                            SType secondSType,
                                            BitSet acceptedRows,
                                            Map<String, Map<String, Integer>> resultMap) {
    boolean firstCoded = (firstSType == SType.ENUM || firstSType == SType.HISTOGRAM);
    boolean secondCoded = (secondSType == SType.ENUM || secondSType == SType.HISTOGRAM);

    String[] firstStr = null;
    String[] secondStr = null;
    int[] firstInt = null;
    int[] secondInt = null;

    try {
      if (firstCoded) {
        firstInt = storageManager.readIntValues(context, firstGrpBy, firstSType);
      } else {
        firstStr = storageManager.readStringValues(context, firstGrpBy, firstSType);
      }
    } catch (Exception e) {
      log.error("Failed to fetch first column '{}' using SType '{}': {}",
                firstGrpBy.getColName(), firstSType, e.toString(), e);
      return;
    }

    try {
      if (secondCoded) {
        secondInt = storageManager.readIntValues(context, secondGrpBy, secondSType);
      } else {
        secondStr = storageManager.readStringValues(context, secondGrpBy, secondSType);
      }
    } catch (Exception e) {
      log.error("Failed to fetch second column '{}' using SType '{}': {}",
                secondGrpBy.getColName(), secondSType, e.toString(), e);
      return;
    }

    int n = context.getTimestamps().length;
    int l1 = firstCoded ? (firstInt != null ? firstInt.length : 0) : (firstStr != null ? firstStr.length : 0);
    int l2 = secondCoded ? (secondInt != null ? secondInt.length : 0) : (secondStr != null ? secondStr.length : 0);
    int limit = Math.min(n, Math.min(l1, l2));
    if (limit == 0) return;

    Map<Integer, String> firstCodeToString = firstCoded ? new HashMap<>() : null;
    Map<Integer, String> secondCodeToString = secondCoded ? new HashMap<>() : null;

    for (int i = acceptedRows.nextSetBit(0); i >= 0 && i < limit; i = acceptedRows.nextSetBit(i + 1)) {
      String v1 = firstCoded
          ? firstCodeToString.computeIfAbsent(firstInt[i], k -> context.getConverter().convertIntToRaw(k, firstGrpBy))
          : safe(firstStr[i]);

      String v2 = secondCoded
          ? secondCodeToString.computeIfAbsent(secondInt[i], k -> context.getConverter().convertIntToRaw(k, secondGrpBy))
          : safe(secondStr[i]);

      setMapValue(resultMap, v1, v2, 1);
    }
  }

  private static String safe(String s) { return s == null ? "" : s; }

  @Override
  public List<GanttColumnCount> getGanttCount(String tableName,
                                              CProfile firstGrpBy,
                                              CProfile secondGrpBy,
                                              CompositeFilter compositeFilter,
                                              int batchSize,
                                              long begin,
                                              long end) throws SqlColMetadataException, BeginEndWrongOrderException {
    if (firstGrpBy.getCsType().isTimeStamp() || secondGrpBy.getCsType().isTimeStamp()) {
      throw new SqlColMetadataException("Group by not supported for timestamp column..");
    }
    if (begin > end) {
      throw new BeginEndWrongOrderException("Begin value must be less the end one..");
    }
    log.info("First column profile: {}", firstGrpBy);
    log.info("Second column profile: {}", secondGrpBy);

    List<Map.Entry<Long, Long>> ranges = new ArrayList<>();
    long totalRange = end - begin + 1;
    long batchTimestampSize = totalRange / batchSize;
    for (int i = 0; i < batchSize; i++) {
      long batchStart = begin + i * batchTimestampSize;
      long batchEnd = (i == batchSize - 1) ? end : batchStart + batchTimestampSize - 1;
      log.info("Processing batch {} --- {}", toLocalDateTime(batchStart), toLocalDateTime(batchEnd));
      ranges.add(Map.entry(batchStart, batchEnd));
    }

    List<GanttTask> tasks = new ArrayList<>();
    for (Map.Entry<Long, Long> entry : ranges) {
      tasks.add(new GanttTask(tableName, firstGrpBy, secondGrpBy, compositeFilter, entry.getKey(), entry.getValue()));
    }

    try (ForkJoinPool pool = new ForkJoinPool()) {
      for (GanttTask task : tasks) {
        pool.execute(task);
      }

      pool.shutdown();
      try {
        if (!pool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
          log.error("Pool did not terminate");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.error("Interrupted while waiting for task completion", e);
        pool.shutdownNow();
      }
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

    if (CType.STRING.equals(secondGrpBy.getCsType().getCType())) {
      log.warn("Not supported for String data type for second group by column");
      return Collections.emptyList();
    }

    BType bType = metaModelApi.getBackendType(tableName);

    if (!BType.BERKLEYDB.equals(bType)) {
      CProfile tsProfile = metaModelApi.getTimestampCProfile(tableName);
      return rawDAO.getGanttSum(tableName, tsProfile, firstGrpBy, secondGrpBy, compositeFilter, begin, end);
    }

    return getListGanttSumLocal(tableName, firstGrpBy, secondGrpBy, compositeFilter, begin, end);
  }

  private List<GanttColumnSum> getListGanttSumLocal(String tableName,
                                                    CProfile firstGrpBy,
                                                    CProfile secondGrpBy,
                                                    CompositeFilter compositeFilter,
                                                    long begin,
                                                    long end) {
    byte tableId = metaModelApi.getTableId(tableName);
    int tsColId = metaModelApi.getTimestampCProfile(tableName).getColId();

    long previousBlockId = this.rawDAO.getPreviousBlockId(tableId, begin);
    Map.Entry<MetadataKey, MetadataKey> keyEntry = getMetadataKeyPair(tableId, begin, end, previousBlockId);

    Map<String, Double> resultMap = new HashMap<>();

    try (EntityCursor<Metadata> cursor = rawDAO.getMetadataEntityCursor(keyEntry.getKey(), keyEntry.getValue())) {
      Metadata columnKey;

      while ((columnKey = cursor.next()) != null) {
        long blockId = columnKey.getMetadataKey().getBlockId();
        long[] timestamps = rawDAO.getRawLong(tableId, blockId, tsColId);

        StorageContext context = StorageContext.builder()
            .tableId(tableId)
            .blockId(blockId)
            .timestamps(timestamps)
            .rawDAO(rawDAO)
            .enumDAO(enumDAO)
            .histogramDAO(histogramDAO)
            .converter(converter)
            .build();

        SType firstSType = getSType(firstGrpBy.getColId(), columnKey);
        SType secondSType = getSType(secondGrpBy.getColId(), columnKey);

        Map<FilterCondition, String[]> filterCache = buildFilterCacheWithFormats(compositeFilter, context, columnKey);
        BitSet accepted = acceptedRows(timestamps, begin, end, compositeFilter, filterCache);
        if (accepted.isEmpty()) continue;

        boolean firstCoded = (firstSType == SType.ENUM || firstSType == SType.HISTOGRAM);

        String[] firstStr = null;
        int[] firstInt = null;
        try {
          if (firstCoded) {
            firstInt = storageManager.readIntValues(context, firstGrpBy, firstSType);
          } else {
            firstStr = storageManager.readStringValues(context, firstGrpBy, firstSType);
          }
        } catch (Exception e) {
          log.error("Failed to fetch first column '{}' using SType '{}': {}",
                    firstGrpBy.getColName(), firstSType, e.toString(), e);
          continue;
        }

        double[] secondValues;
        try {
          secondValues = storageManager.readDoubleValues(context, secondGrpBy, secondSType);
        } catch (Exception e) {
          log.error("Failed to fetch second column '{}' as doubles using SType '{}': {}",
                    secondGrpBy.getColName(), secondSType, e.toString(), e);
          continue;
        }

        int n = timestamps.length;
        int l1 = firstCoded ? (firstInt != null ? firstInt.length : 0) : (firstStr != null ? firstStr.length : 0);
        int l2 = (secondValues != null ? secondValues.length : 0);
        int limit = Math.min(n, Math.min(l1, l2));
        if (limit == 0) continue;

        Map<Integer, String> firstCodeToString = firstCoded ? new HashMap<>() : null;

        for (int i = accepted.nextSetBit(0); i >= 0 && i < limit; i = accepted.nextSetBit(i + 1)) {
          String groupKey = firstCoded
              ? firstCodeToString.computeIfAbsent(firstInt[i], k -> converter.convertIntToRaw(k, firstGrpBy))
              : safe(firstStr[i]);

          resultMap.merge(groupKey, secondValues[i], Double::sum);
        }
      }
    } catch (Exception e) {
      log.error("Error processing gantt sum data (refactored): ", e);
    }

    Map<String, Double> processedMap = new HashMap<>();
    if (DataType.MAP.equals(firstGrpBy.getCsType().getDType())) {
      handleMapGanttSum(resultMap, processedMap, firstGrpBy, compositeFilter);
      resultMap = processedMap;
    }
    if (DataType.ARRAY.equals(firstGrpBy.getCsType().getDType())) {
      handleArrayGanttSum(resultMap, processedMap, firstGrpBy, compositeFilter);
      resultMap = processedMap;
    }

    List<GanttColumnSum> list = new ArrayList<>();
    resultMap.forEach((k, v) -> list.add(new GanttColumnSum(k, v)));
    return list;
  }

  private void handleMapGanttSum(Map<String, Double> inputMap,
                                 Map<String, Double> outputMap,
                                 CProfile cProfile,
                                 CompositeFilter compositeFilter) {
    Set<String> filterKeys = getFilterDataForProfile(cProfile, compositeFilter);

    for (Map.Entry<String, Double> entry : inputMap.entrySet()) {
      try {
        Map<String, Long> parsedMap = parseStringToTypedMap(entry.getKey(), String::new, Long::parseLong, "=");
        if (parsedMap.isEmpty()) {
          outputMap.merge(Mapper.STRING_NULL, entry.getValue(), Double::sum);
        } else {
          for (Map.Entry<String, Long> mapEntry : parsedMap.entrySet()) {
            String mapKey = mapEntry.getKey();
            if (filterKeys == null || filterKeys.contains(mapKey)) {
              double multiplier = mapEntry.getValue() != null ? mapEntry.getValue().doubleValue() : 1.0;
              double weightedSum = entry.getValue() * multiplier;
              outputMap.merge(mapKey, weightedSum, Double::sum);
            }
          }
        }
      } catch (Exception e) {
        log.warn("Failed to parse map for gantt sum: '{}' for column {}", entry.getKey(), cProfile.getColName(), e);
        if (filterKeys == null) {
          outputMap.merge(entry.getKey(), entry.getValue(), Double::sum);
        }
      }
    }
  }

  private void handleArrayGanttSum(Map<String, Double> inputMap,
                                   Map<String, Double> outputMap,
                                   CProfile cProfile,
                                   CompositeFilter compositeFilter) {
    Set<String> filterElements = getFilterDataForProfile(cProfile, compositeFilter);

    for (Map.Entry<String, Double> entry : inputMap.entrySet()) {
      try {
        String[] array = parseStringToTypedArray(entry.getKey(), ",");
        if (array.length == 0) {
          outputMap.merge(Mapper.STRING_NULL, entry.getValue(), Double::sum);
        } else {
          double distributedValue = entry.getValue() / array.length;
          for (String element : array) {
            String trimmedElement = element.trim();
            if (!trimmedElement.isEmpty()) {
              if (filterElements == null || filterElements.contains(trimmedElement)) {
                outputMap.merge(trimmedElement, distributedValue, Double::sum);
              }
            }
          }
        }
      } catch (Exception e) {
        log.warn("Failed to parse array for gantt sum: '{}' for column {}", entry.getKey(), cProfile.getColName(), e);
        if (filterElements == null) {
          outputMap.merge(entry.getKey(), entry.getValue(), Double::sum);
        }
      }
    }
  }

  private Map<FilterCondition, String[]> buildFilterCache(CompositeFilter filter,
                                                          byte tableId,
                                                          long blockId,
                                                          long[] timestamps) {
    Map<FilterCondition, String[]> cache = new HashMap<>();
    if (filter == null) return cache;
    for (FilterCondition c : filter.getConditions()) {
      String[] arr = getStringArrayValues(rawDAO, enumDAO, histogramDAO, converter,
                                          tableId, c.getCProfile(), blockId, timestamps);
      cache.put(c, arr);
    }
    return cache;
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

    Set<String> distinctSet = new LinkedHashSet<>();
    byte tableId = metaModelApi.getTableId(tableName);
    int tsColId = tsProfile.getColId();

    long previousBlockId = this.rawDAO.getPreviousBlockId(tableId, begin);
    Map.Entry<MetadataKey, MetadataKey> keyEntry = getMetadataKeyPair(tableId, begin, end, previousBlockId);

    try (EntityCursor<Metadata> cursor = rawDAO.getMetadataEntityCursor(keyEntry.getKey(), keyEntry.getValue())) {
      Metadata columnKey;
      while ((columnKey = cursor.next()) != null) {
        long blockId = columnKey.getMetadataKey().getBlockId();
        long[] timestamps = rawDAO.getRawLong(tableId, blockId, tsColId);

        StorageContext context = StorageContext.builder()
            .tableId(tableId)
            .blockId(blockId)
            .timestamps(timestamps)
            .rawDAO(rawDAO)
            .enumDAO(enumDAO)
            .histogramDAO(histogramDAO)
            .converter(converter)
            .build();

        Map<FilterCondition, String[]> filterCache = buildFilterCacheWithFormats(compositeFilter, context, columnKey);
        BitSet accepted = acceptedRows(timestamps, begin, end, compositeFilter, filterCache);
        if (accepted.isEmpty()) continue;

        SType sType = getSType(cProfile.getColId(), columnKey);
        String[] values;
        try {
          values = storageManager.readStringValues(context, cProfile, sType);
        } catch (Exception e) {
          log.error("Failed to fetch distinct values for '{}' using SType '{}': {}",
                    cProfile.getColName(), sType, e.toString(), e);
          continue;
        }

        DataType dataType = cProfile.getCsType().getDType();
        for (int i = accepted.nextSetBit(0); i >= 0; i = accepted.nextSetBit(i + 1)) {
          if (i >= values.length) break;
          String v = values[i];
          v = formatFloatingPoint(v, cProfile.getCsType().getCType());

          if (DataType.MAP.equals(dataType)) {
            processMapValueForDistinct(v, distinctSet, cProfile, compositeFilter);
          } else if (DataType.ARRAY.equals(dataType)) {
            processArrayValueForDistinct(v, distinctSet, cProfile, compositeFilter);
          } else {
            distinctSet.add(v);
          }
        }
      }
    } catch (Exception e) {
      log.error("Error processing distinct data with composite filter (refactored)", e);
    }

    List<String> resultList = new ArrayList<>(distinctSet);

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

  /**
   * Build filter cache via StorageManager, similar to getGanttCount approach.
   */
  private Map<FilterCondition, String[]> buildFilterCacheWithFormats(CompositeFilter compositeFilter,
                                                                     StorageContext context,
                                                                     Metadata columnKey) {
    Map<FilterCondition, String[]> cache = new HashMap<>();
    if (compositeFilter == null) return cache;

    for (FilterCondition c : compositeFilter.getConditions()) {
      try {
        SType sType = getSType(c.getCProfile().getColId(), columnKey);
        String[] vals = storageManager.readStringValues(context, c.getCProfile(), sType);
        cache.put(c, vals);
      } catch (Exception e) {
        log.error("Failed to build filter cache for column '{}' using unified StorageManager: {}",
                  c.getCProfile().getColName(), e.toString(), e);
      }
    }
    return cache;
  }

  private void processMapValueForDistinct(String value, Set<String> distinctSet, CProfile cProfile, CompositeFilter compositeFilter) {
    try {
      Map<String, Long> parsedMap = parseStringToTypedMap(value, String::new, Long::parseLong, "=");
      Set<String> filterKeys = getFilterDataForProfile(cProfile, compositeFilter);

      for (String mapKey : parsedMap.keySet()) {
        if (filterKeys == null || filterKeys.contains(mapKey)) {
          distinctSet.add(mapKey);
        }
      }
    } catch (Exception e) {
      log.warn("Failed to parse map value for distinct: '{}'", value, e);
      if (getFilterDataForProfile(cProfile, compositeFilter) == null) {
        distinctSet.add(value);
      }
    }
  }

  private void processArrayValueForDistinct(String value, Set<String> distinctSet, CProfile cProfile, CompositeFilter compositeFilter) {
    try {
      String[] array = parseStringToTypedArray(value, ",");
      Set<String> filterElements = getFilterDataForProfile(cProfile, compositeFilter);

      for (String element : array) {
        String trimmedElement = element.trim();
        if (!trimmedElement.isEmpty() && (filterElements == null || filterElements.contains(trimmedElement))) {
          distinctSet.add(trimmedElement);
        }
      }
    } catch (Exception e) {
      log.warn("Failed to parse array value for distinct: '{}'", value, e);
      if (getFilterDataForProfile(cProfile, compositeFilter) == null) {
        distinctSet.add(value);
      }
    }
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
      log.info("Start task at: {}", LocalDateTime.now());
      try {
        ganttColumnCountList = getGanttCount(tableName, firstGrpBy, secondGrpBy, compositeFilter, begin, end);
      } catch (SqlColMetadataException e) {
        throw new RuntimeException(e);
      }
      log.info("End task at: {}", LocalDateTime.now());
    }
  }
}