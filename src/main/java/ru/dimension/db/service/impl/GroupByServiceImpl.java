package ru.dimension.db.service.impl;

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
import java.util.OptionalDouble;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
import ru.dimension.db.storage.Converter;
import ru.dimension.db.storage.EnumDAO;
import ru.dimension.db.storage.HistogramDAO;
import ru.dimension.db.storage.RawDAO;
import ru.dimension.db.storage.bdb.entity.Metadata;
import ru.dimension.db.storage.bdb.entity.MetadataKey;
import ru.dimension.db.storage.bdb.entity.column.EColumn;
import ru.dimension.db.storage.helper.EnumHelper;

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

    if (GroupFunction.COUNT.equals(groupFunction)) {
      return this.getListStackedColumnCountWithCompositeFilter(tableName, tsProfile, cProfile, compositeFilter, begin, end);
    } else if (GroupFunction.SUM.equals(groupFunction)) {
      return this.getListStackedColumnSumWithCompositeFilter(tableName, tsProfile, cProfile, compositeFilter, begin, end);
    } else if (GroupFunction.AVG.equals(groupFunction)) {
      return this.getListStackedColumnAvgWithCompositeFilter(tableName, tsProfile, cProfile, compositeFilter, begin, end);
    } else {
      throw new RuntimeException("Group function not supported: " + groupFunction);
    }
  }

  private List<StackedColumn> getListStackedColumnCountWithCompositeFilter(String tableName,
                                                                           CProfile tsProfile,
                                                                           CProfile cProfile,
                                                                           CompositeFilter compositeFilter,
                                                                           long begin,
                                                                           long end) {
    BType bType = metaModelApi.getBackendType(tableName);

    if (!BType.BERKLEYDB.equals(bType)) {
      return rawDAO.getStacked(tableName, tsProfile, cProfile, GroupFunction.COUNT, compositeFilter, begin, end);
    }

    byte tableId = metaModelApi.getTableId(tableName);
    List<StackedColumn> list = new ArrayList<>();

    long previousBlockId = this.rawDAO.getPreviousBlockId(tableId, begin);
    if (previousBlockId != begin && previousBlockId != 0) {
      processBlockWithCompositeFilter(tableId, tsProfile, cProfile, compositeFilter, previousBlockId, begin, end, list);
    }

    this.rawDAO.getListBlockIds(tableId, begin, end).forEach(blockId ->
                                                                 processBlockWithCompositeFilter(tableId, tsProfile, cProfile, compositeFilter, blockId, begin, end, list)
    );

    if (DataType.MAP.equals(cProfile.getCsType().getDType())) {
      return handleMap(list);
    } else if (DataType.ARRAY.equals(cProfile.getCsType().getDType())) {
      return handleArray(list);
    }

    return list;
  }

  private List<StackedColumn> getListStackedColumnSumWithCompositeFilter(String tableName,
                                                                         CProfile tsProfile,
                                                                         CProfile cProfile,
                                                                         CompositeFilter compositeFilter,
                                                                         long begin,
                                                                         long end) {
    BType bType = metaModelApi.getBackendType(tableName);

    if (!BType.BERKLEYDB.equals(bType)) {
      return rawDAO.getStacked(tableName, tsProfile, cProfile, GroupFunction.SUM, compositeFilter, begin, end);
    }

    byte tableId = metaModelApi.getTableId(tableName);

    StackedColumn stackedColumn = StackedColumn.builder().key(begin).tail(end).build();
    stackedColumn.setKeySum(new HashMap<>());

    List<Object> columnData = getColumnDataWithCompositeFilter(tableId, tsProfile, cProfile, compositeFilter, begin, end);

    double sum = columnData.stream()
        .filter(item -> item != null && !((String) item).isEmpty())
        .mapToDouble(item -> Double.parseDouble((String) item))
        .sum();

    stackedColumn.getKeySum().put(cProfile.getColName(), sum);

    return List.of(stackedColumn);
  }

  private List<StackedColumn> getListStackedColumnAvgWithCompositeFilter(String tableName,
                                                                         CProfile tsProfile,
                                                                         CProfile cProfile,
                                                                         CompositeFilter compositeFilter,
                                                                         long begin,
                                                                         long end) {
    BType bType = metaModelApi.getBackendType(tableName);

    if (!BType.BERKLEYDB.equals(bType)) {
      return rawDAO.getStacked(tableName, tsProfile, cProfile, GroupFunction.AVG, compositeFilter, begin, end);
    }

    byte tableId = metaModelApi.getTableId(tableName);

    StackedColumn stackedColumn = StackedColumn.builder().key(begin).tail(end).build();
    stackedColumn.setKeyAvg(new HashMap<>());

    List<Object> columnData = getColumnDataWithCompositeFilter(tableId, tsProfile, cProfile, compositeFilter, begin, end);

    OptionalDouble average = columnData.stream()
        .filter(item -> item != null && !((String) item).isEmpty())
        .mapToDouble(item -> Double.parseDouble((String) item))
        .average();

    stackedColumn.getKeyAvg().put(cProfile.getColName(), average.orElse(0.0));

    return List.of(stackedColumn);
  }

  private List<Object> getColumnDataWithCompositeFilter(byte tableId,
                                                        CProfile tsProfile,
                                                        CProfile cProfile,
                                                        CompositeFilter compositeFilter,
                                                        long begin,
                                                        long end) {
    List<Object> columnData = new ArrayList<>();

    long previousBlockId = this.rawDAO.getPreviousBlockId(tableId, begin);
    if (previousBlockId != begin && previousBlockId != 0) {
      this.computeFilteredRawDataWithCompositeFilter(tableId, tsProfile, cProfile, compositeFilter, previousBlockId, begin, end, columnData);
    }

    this.rawDAO.getListBlockIds(tableId, begin, end)
        .forEach(blockId ->
                     this.computeFilteredRawDataWithCompositeFilter(tableId, tsProfile, cProfile, compositeFilter, blockId, begin, end, columnData));

    return columnData;
  }

  private void computeFilteredRawDataWithCompositeFilter(byte tableId,
                                                         CProfile tsProfile,
                                                         CProfile cProfile,
                                                         CompositeFilter compositeFilter,
                                                         long blockId,
                                                         long begin,
                                                         long end,
                                                         List<Object> columnData) {
    long[] timestamps = rawDAO.getRawLong(tableId, blockId, tsProfile.getColId());

    // Get target column values
    String[] targetColumnValues = getStringArrayValues(rawDAO, enumDAO, histogramDAO, converter, tableId, cProfile, blockId, timestamps);

    // Get values for filter conditions
    Map<CProfile, String[]> filterValuesMap = new HashMap<>();
    if (compositeFilter != null) {
      for (FilterCondition condition : compositeFilter.getConditions()) {
        String[] conditionValues = getStringArrayValues(rawDAO, enumDAO, histogramDAO, converter,
                                                        tableId, condition.getCProfile(), blockId, timestamps);
        filterValuesMap.put(condition.getCProfile(), conditionValues);
      }
    }

    for (int i = 0; i < timestamps.length; i++) {
      if (timestamps[i] >= begin && timestamps[i] <= end) {
        boolean shouldInclude = true;

        if (compositeFilter != null) {
          // Build values array in correct order for filter conditions - FIXED ORDER
          String[] filterValues = new String[compositeFilter.getConditions().size()];
          int idx = 0;
          for (FilterCondition condition : compositeFilter.getConditions()) {
            filterValues[idx++] = filterValuesMap.get(condition.getCProfile())[i];
          }
          shouldInclude = compositeFilter.test(filterValues);
        }

        if (shouldInclude) {
          columnData.add(targetColumnValues[i]);
        }
      }
    }
  }

  private void processBlockWithCompositeFilter(byte tableId,
                                               CProfile tsProfile,
                                               CProfile cProfile,
                                               CompositeFilter compositeFilter,
                                               long blockId,
                                               long begin,
                                               long end,
                                               List<StackedColumn> list) {
    try {
      long[] timestamps = rawDAO.getRawLong(tableId, blockId, tsProfile.getColId());

      // Get the target column value
      String[] targetColumnValues = getStringArrayValues(
          rawDAO, enumDAO, histogramDAO, converter,
          tableId, cProfile, blockId, timestamps
      );

      // Only get filter values if we have a composite filter
      Map<CProfile, String[]> filterColumnValuesMap = new HashMap<>();
      if (compositeFilter != null) {
        for (FilterCondition condition : compositeFilter.getConditions()) {
          String[] conditionValues = getStringArrayValues(
              rawDAO, enumDAO, histogramDAO, converter,
              tableId, condition.getCProfile(), blockId, timestamps
          );
          filterColumnValuesMap.put(condition.getCProfile(), conditionValues);
        }
      }

      Map<String, Integer> valueCounts = new LinkedHashMap<>();
      long lastTimestamp = timestamps.length > 0 ? timestamps[timestamps.length - 1] : 0;

      for (int i = 0; i < timestamps.length; i++) {
        long timestamp = timestamps[i];

        if (timestamp >= begin && timestamp <= end) {
          boolean includeRow = true;

          // Apply composite filter if present
          if (compositeFilter != null) {
            // Prepare values array for composite filter evaluation - FIXED ORDER
            String[] valuesForFilter = new String[compositeFilter.getConditions().size()];
            int index = 0;
            for (FilterCondition condition : compositeFilter.getConditions()) {
              valuesForFilter[index++] = filterColumnValuesMap.get(condition.getCProfile())[i];
            }

            includeRow = compositeFilter.test(valuesForFilter);
          }

          if (includeRow) {
            String value = targetColumnValues[i] != null ? targetColumnValues[i] : "";
            valueCounts.compute(value, (k, count) -> count == null ? 1 : count + 1);
          }
        }
      }

      if (!valueCounts.isEmpty()) {
        StackedColumn column = StackedColumn.builder()
            .key(blockId)
            .tail(lastTimestamp)
            .keyCount(valueCounts)
            .build();
        list.add(column);
      }
    } catch (Exception e) {
      log.error("Error processing block {}: {}", blockId, e.getMessage(), e);
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
    int firstColId = firstGrpBy.getColId();
    int secondColId = secondGrpBy.getColId();

    long previousBlockId = this.rawDAO.getPreviousBlockId(tableId, begin);
    Map.Entry<MetadataKey, MetadataKey> keyEntry = getMetadataKeyPair(tableId, begin, end, previousBlockId);

    Map<String, Map<String, Integer>> mapFinal = new HashMap<>();

    try (EntityCursor<Metadata> cursor = rawDAO.getMetadataEntityCursor(keyEntry.getKey(), keyEntry.getValue())) {
      Metadata columnKey;

      while ((columnKey = cursor.next()) != null) {
        long blockId = columnKey.getMetadataKey().getBlockId();

        SType firstSType = getSType(firstColId, columnKey);
        SType secondSType = getSType(secondColId, columnKey);

        if (checkSTypeILocal(firstSType, secondSType, SType.HISTOGRAM, SType.HISTOGRAM)) {
          Map<Integer, Map<Integer, Integer>> map = new HashMap<>();
          this.computeHistHist(tableId, blockId, firstGrpBy, secondGrpBy, tsColId, begin, end, compositeFilter, map);

          map.forEach((key, value) -> value.forEach((kVal, vVal) -> setMapValue(mapFinal,
                                                                                converter.convertIntToRaw(key, firstGrpBy),
                                                                                converter.convertIntToRaw(kVal, secondGrpBy), vVal)));

        } else if (checkSTypeILocal(firstSType, secondSType, SType.ENUM, SType.ENUM)) {
          Map<Integer, Map<Integer, Integer>> map = new HashMap<>();
          this.computeEnumEnum(tableId, tsColId, blockId, firstGrpBy, secondGrpBy, begin, end, compositeFilter, map);

          map.forEach((key, value) -> value.forEach((kVal, vVal) -> setMapValue(mapFinal,
                                                                                converter.convertIntToRaw(key, firstGrpBy),
                                                                                converter.convertIntToRaw(kVal, secondGrpBy), vVal)));

        } else if (checkSTypeILocal(firstSType, secondSType, SType.RAW, SType.RAW)) {
          Map<String, Map<String, Integer>> map = new HashMap<>();
          this.computeRawRaw(tableId, firstGrpBy, secondGrpBy, tsColId, blockId, begin, end, compositeFilter, map);

          map.forEach((key, value) -> value.forEach((kVal, vVal) -> setMapValue(mapFinal, key, kVal, vVal)));

        } else if (checkSTypeILocal(firstSType, secondSType, SType.HISTOGRAM, SType.ENUM)) {
          Map<Integer, Map<Integer, Integer>> map = new HashMap<>();
          this.computeHistEnum(tableId, firstGrpBy, secondGrpBy, tsColId, blockId, begin, end, compositeFilter, map);

          map.forEach((key, value) -> value.forEach((kVal, vVal) -> setMapValue(mapFinal,
                                                                                converter.convertIntToRaw(key, firstGrpBy),
                                                                                converter.convertIntToRaw(kVal, secondGrpBy), vVal)));

        } else if (checkSTypeILocal(firstSType, secondSType, SType.ENUM, SType.HISTOGRAM)) {
          Map<Integer, Map<Integer, Integer>> map = new HashMap<>();
          this.computeEnumHist(tableId, firstGrpBy, secondGrpBy, tsColId, blockId, begin, end, compositeFilter, map);

          map.forEach((key, value) -> value.forEach((kVal, vVal) -> setMapValue(mapFinal,
                                                                                converter.convertIntToRaw(key, firstGrpBy),
                                                                                converter.convertIntToRaw(kVal, secondGrpBy), vVal)));

        } else if (checkSTypeILocal(firstSType, secondSType, SType.HISTOGRAM, SType.RAW)) {
          Map<Integer, Map<String, Integer>> map = new HashMap<>();
          this.computeHistRaw(tableId, firstGrpBy, secondGrpBy, tsColId, blockId, begin, end, compositeFilter, map);

          map.forEach((key, value) -> value.forEach((kVal, vVal) -> setMapValue(mapFinal,
                                                                                converter.convertIntToRaw(key, firstGrpBy), kVal, vVal)));

        } else if (checkSTypeILocal(firstSType, secondSType, SType.RAW, SType.HISTOGRAM)) {
          Map<String, Map<Integer, Integer>> map = new HashMap<>();
          this.computeRawHist(tableId, firstGrpBy, secondGrpBy, tsColId, blockId, begin, end, compositeFilter, map);

          map.forEach((key, value) -> value.forEach((kVal, vVal) -> setMapValue(mapFinal,
                                                                                key, converter.convertIntToRaw(kVal, secondGrpBy), vVal)));

        } else if (checkSTypeILocal(firstSType, secondSType, SType.ENUM, SType.RAW)) {
          Map<Integer, Map<String, Integer>> map = new HashMap<>();
          this.computeEnumRaw(tableId, firstGrpBy, secondGrpBy, tsColId, blockId, begin, end, compositeFilter, map);

          map.forEach((key, value) -> value.forEach((kVal, vVal) -> setMapValue(mapFinal,
                                                                                converter.convertIntToRaw(key, firstGrpBy), kVal, vVal)));

        } else if (checkSTypeILocal(firstSType, secondSType, SType.RAW, SType.ENUM)) {
          Map<String, Map<Integer, Integer>> map = new HashMap<>();
          this.computeRawEnum(tableId, firstGrpBy, secondGrpBy, tsColId, blockId, begin, end, compositeFilter, map);

          map.forEach((key, value) -> value.forEach((kVal, vVal) -> setMapValue(mapFinal,
                                                                                key, converter.convertIntToRaw(kVal, secondGrpBy), vVal)));

        }
      }

    } catch (Exception e) {
      log.error(e.getMessage());
    }

    if (DataType.ARRAY.equals(firstGrpBy.getCsType().getDType()) || DataType.ARRAY.equals(secondGrpBy.getCsType().getDType())) {
      Map<String, Map<String, Integer>> map = handleArray(firstGrpBy, secondGrpBy, mapFinal);
      mapFinal.clear();
      mapFinal.putAll(map);
    }

    if (DataType.MAP.equals(firstGrpBy.getCsType().getDType()) || DataType.MAP.equals(secondGrpBy.getCsType().getDType())) {
      return handleMap(firstGrpBy, secondGrpBy, mapFinal);
    }

    List<GanttColumnCount> list = new ArrayList<>();

    mapFinal.forEach((key, value) -> list.add(GanttColumnCount.builder().key(key).gantt(value).build()));

    return list;
  }

  private void computeEnumEnum(byte tableId,
                               int tsColId,
                               long blockId,
                               CProfile firstGrpBy,
                               CProfile secondGrpBy,
                               long begin,
                               long end,
                               CompositeFilter compositeFilter,
                               Map<Integer, Map<Integer, Integer>> map) {
    long[] timestamp = rawDAO.getRawLong(tableId, blockId, tsColId);

    Map<FilterCondition, String[]> cache = buildFilterCache(compositeFilter, tableId, blockId, timestamp);
    BitSet acc = acceptedRows(timestamp, begin, end, compositeFilter, cache);

    if (acc.isEmpty()) {
      return;
    }

    Map.Entry<int[], byte[]> listFirst = computeEnumEnumBlock(tableId, firstGrpBy, timestamp, blockId, acc);
    Map.Entry<int[], byte[]> listSecond = computeEnumEnumBlock(tableId, secondGrpBy, timestamp, blockId, acc);

    if (listFirst.getValue().length > 0 && listSecond.getValue().length > 0) {
      for (int i = 0; i < timestamp.length; i++) {
        if (timestamp[i] >= begin && timestamp[i] <= end && acc.get(i)) {
          int intToRawFirst = EnumHelper.getIndexValue(listFirst.getKey(), listFirst.getValue()[i]);
          int intToRawSecond = EnumHelper.getIndexValue(listSecond.getKey(), listSecond.getValue()[i]);
          setMapValueEnumBlock(map, intToRawFirst, intToRawSecond, 1);
        }
      }
    }
  }

  private void computeRawRaw(byte tableId,
                             CProfile firstGrpBy,
                             CProfile secondGrpBy,
                             int tsColId,
                             long blockId,
                             long begin,
                             long end,
                             CompositeFilter compositeFilter,
                             Map<String, Map<String, Integer>> map) {

    long[] timestamps = rawDAO.getRawLong(tableId, blockId, tsColId);
    String[] first = getStringArrayValuesRaw(rawDAO, tableId, blockId, firstGrpBy);
    String[] second = getStringArrayValuesRaw(rawDAO, tableId, blockId, secondGrpBy);

    Map<FilterCondition, String[]> cache = buildFilterCache(compositeFilter, tableId, blockId, timestamps);
    BitSet acc = acceptedRows(timestamps, begin, end, compositeFilter, cache);

    if (acc.isEmpty()) {
      return;
    }

    if (first.length != 0 && second.length != 0) {
      acc.stream().forEach(i -> setMapValue(map, first[i], second[i], 1));
    }
  }

  private void computeHistEnum(byte tableId,
                               CProfile firstGrpBy,
                               CProfile secondGrpBy,
                               int tsColId,
                               long blockId,
                               long begin,
                               long end,
                               CompositeFilter compositeFilter,
                               Map<Integer, Map<Integer, Integer>> map) {
    long[] timestamp = rawDAO.getRawLong(tableId, blockId, tsColId);

    Map<FilterCondition, String[]> cache = buildFilterCache(compositeFilter, tableId, blockId, timestamp);
    BitSet acc = acceptedRows(timestamp, begin, end, compositeFilter, cache);

    if (acc.isEmpty()) {
      return;
    }

    int[][] histograms = histogramDAO.get(tableId, blockId, firstGrpBy.getColId());
    int[] unpackedHistogram = getHistogramUnPack(timestamp, histograms);

    Map.Entry<int[], byte[]> enumData = computeEnumEnumBlock(tableId, secondGrpBy, timestamp, blockId, acc);

    if (unpackedHistogram.length > 0 && enumData.getValue().length > 0) {
      for (int i = 0; i < timestamp.length; i++) {
        if (acc.get(i)) {
          int histValue = unpackedHistogram[i];
          int enumValue = EnumHelper.getIndexValue(enumData.getKey(), enumData.getValue()[i]);
          setMapValue(map, histValue, enumValue, 1);
        }
      }
    }
  }

  private void computeEnumHist(byte tableId,
                               CProfile firstGrpBy,
                               CProfile secondGrpBy,
                               int tsColId,
                               long blockId,
                               long begin,
                               long end,
                               CompositeFilter compositeFilter,
                               Map<Integer, Map<Integer, Integer>> map) {
    long[] timestamp = rawDAO.getRawLong(tableId, blockId, tsColId);

    Map<FilterCondition, String[]> cache = buildFilterCache(compositeFilter, tableId, blockId, timestamp);
    BitSet acc = acceptedRows(timestamp, begin, end, compositeFilter, cache);

    if (acc.isEmpty()) {
      return;
    }

    Map.Entry<int[], byte[]> enumData = computeEnumEnumBlock(tableId, firstGrpBy, timestamp, blockId, acc);

    int[][] histograms = histogramDAO.get(tableId, blockId, secondGrpBy.getColId());
    int[] unpackedHistogram = getHistogramUnPack(timestamp, histograms);

    if (enumData.getValue().length > 0 && unpackedHistogram.length > 0) {
      for (int i = 0; i < timestamp.length; i++) {
        if (acc.get(i)) {
          int enumValue = EnumHelper.getIndexValue(enumData.getKey(), enumData.getValue()[i]);
          int histValue = unpackedHistogram[i];
          setMapValue(map, enumValue, histValue, 1);
        }
      }
    }
  }

  private void computeHistHist(byte tableId,
                               long blockId,
                               CProfile firstGrpBy,
                               CProfile secondGrpBy,
                               int tsColId,
                               long begin,
                               long end,
                               CompositeFilter compositeFilter,
                               Map<Integer, Map<Integer, Integer>> map) {
    long[] timestamps = rawDAO.getRawLong(tableId, blockId, tsColId);

    Map<FilterCondition, String[]> cache = buildFilterCache(compositeFilter, tableId, blockId, timestamps);
    BitSet acc = acceptedRows(timestamps, begin, end, compositeFilter, cache);

    if (acc.isEmpty()) {
      return;
    }

    int[][] firstHist = histogramDAO.get(tableId, blockId, firstGrpBy.getColId());
    int[][] secondHist = histogramDAO.get(tableId, blockId, secondGrpBy.getColId());

    if (firstHist[0].length == 0 || secondHist[0].length == 0) {
      return;
    }

    int[] firstUnpacked = getHistogramUnPack(timestamps, firstHist);
    int[] secondUnpacked = getHistogramUnPack(timestamps, secondHist);

    for (int i = 0; i < timestamps.length; i++) {
      if (acc.get(i)) {
        int valueFirst = firstUnpacked[i];
        int valueSecond = secondUnpacked[i];
        setMapValue(map, valueFirst, valueSecond, 1);
      }
    }
  }

  private void computeHistRaw(byte tableId,
                              CProfile firstGrpBy,
                              CProfile secondGrpBy,
                              int tsColId,
                              long blockId,
                              long begin,
                              long end,
                              CompositeFilter compositeFilter,
                              Map<Integer, Map<String, Integer>> map) {
    long[] timestamp = rawDAO.getRawLong(tableId, blockId, tsColId);

    Map<FilterCondition, String[]> cache = buildFilterCache(compositeFilter, tableId, blockId, timestamp);
    BitSet acc = acceptedRows(timestamp, begin, end, compositeFilter, cache);

    if (acc.isEmpty()) {
      return;
    }

    List<Integer> listFirst = computeHistogramBlock(tableId, firstGrpBy, timestamp, blockId, acc);
    List<String> listSecond = computeRawBlock(tableId, secondGrpBy, timestamp, blockId, acc);

    if (!listFirst.isEmpty() && !listSecond.isEmpty()) {
      setMapValueCommon(map, listFirst, listSecond, 1);
    }
  }

  private void computeRawHist(byte tableId,
                              CProfile firstGrpBy,
                              CProfile secondGrpBy,
                              int tsColId,
                              long blockId,
                              long begin,
                              long end,
                              CompositeFilter compositeFilter,
                              Map<String, Map<Integer, Integer>> map) {
    long[] timestamp = rawDAO.getRawLong(tableId, blockId, tsColId);

    Map<FilterCondition, String[]> cache = buildFilterCache(compositeFilter, tableId, blockId, timestamp);
    BitSet acc = acceptedRows(timestamp, begin, end, compositeFilter, cache);

    if (acc.isEmpty()) {
      return;
    }

    List<String> listFirst = computeRawBlock(tableId, firstGrpBy, timestamp, blockId, acc);
    List<Integer> listSecond = computeHistogramBlock(tableId, secondGrpBy, timestamp, blockId, acc);

    if (!listFirst.isEmpty() && !listSecond.isEmpty()) {
      setMapValueCommon(map, listFirst, listSecond, 1);
    }
  }

  private void computeEnumRaw(byte tableId,
                              CProfile firstGrpBy,
                              CProfile secondGrpBy,
                              int tsColId,
                              long blockId,
                              long begin,
                              long end,
                              CompositeFilter compositeFilter,
                              Map<Integer, Map<String, Integer>> map) {
    long[] timestamp = rawDAO.getRawLong(tableId, blockId, tsColId);

    Map<FilterCondition, String[]> cache = buildFilterCache(compositeFilter, tableId, blockId, timestamp);
    BitSet acc = acceptedRows(timestamp, begin, end, compositeFilter, cache);

    if (acc.isEmpty()) {
      return;
    }

    String[] secondColumnValues = getStringArrayValuesRaw(rawDAO, tableId, blockId, secondGrpBy);

    Map.Entry<int[], byte[]> listFirst = computeEnumEnumBlock(tableId, firstGrpBy, timestamp, blockId, acc);

    if (listFirst.getValue().length > 0 && secondColumnValues.length > 0) {
      for (int i = 0; i < timestamp.length; i++) {
        if (acc.get(i)) {
          int intToRawFirst = EnumHelper.getIndexValue(listFirst.getKey(), listFirst.getValue()[i]);
          String rawValue = secondColumnValues[i];
          setMapValueRawEnumBlock(map, intToRawFirst, rawValue, 1);
        }
      }
    }
  }

  private void computeRawEnum(byte tableId,
                              CProfile firstGrpBy,
                              CProfile secondGrpBy,
                              int tsColId,
                              long blockId,
                              long begin,
                              long end,
                              CompositeFilter compositeFilter,
                              Map<String, Map<Integer, Integer>> map) {
    long[] timestamp = rawDAO.getRawLong(tableId, blockId, tsColId);

    Map<FilterCondition, String[]> cache = buildFilterCache(compositeFilter, tableId, blockId, timestamp);
    BitSet acc = acceptedRows(timestamp, begin, end, compositeFilter, cache);

    if (acc.isEmpty()) {
      return;
    }

    List<String> listFirst = computeRawBlock(tableId, firstGrpBy, timestamp, blockId, acc);
    Map.Entry<int[], byte[]> listSecond = computeEnumBlock(tableId, secondGrpBy, timestamp, blockId, acc);

    setMapValueRawEnumBlock(map, listFirst, listSecond, 1);
  }


  private List<Integer> computeHistogramBlock(byte tableId,
                                              CProfile cProfile,
                                              long[] timestamp,
                                              long blockId,
                                              BitSet accepted) {
    List<Integer> list = new ArrayList<>();
    int[][] h = histogramDAO.get(tableId, blockId, cProfile.getColId());
    for (int i = 0; i < h[0].length; i++) {
      int fNextIndex = getNextIndex(i, h, timestamp);
      int startIndex = (i == 0) ? 0
          : fNextIndex - (fNextIndex - getNextIndex(i - 1, h, timestamp)) + 1;
      for (int k = startIndex; k <= fNextIndex; k++) {
        if (accepted.get(k)) list.add(h[1][i]);
      }
    }
    return list;
  }

  private Map.Entry<int[], byte[]> computeEnumBlock(byte tableId,
                                                    CProfile cProfile,
                                                    long[] timestamp,
                                                    long blockId,
                                                    BitSet accepted) {
    byte[] eBytes = new byte[timestamp.length];
    EColumn eColumn = enumDAO.getEColumnValues(tableId, blockId, cProfile.getColId());
    IntStream.range(0, timestamp.length)
        .filter(accepted::get)
        .forEach(iR -> eBytes[iR] = eColumn.getDataByte()[iR]);

    int count = (int) IntStream.range(0, timestamp.length)
        .filter(accepted::get)
        .count();

    byte[] filtered = new byte[count];
    int index = 0;
    for (int i = 0; i < timestamp.length; i++) {
      if (accepted.get(i)) {
        filtered[index++] = eBytes[i];
      }
    }

    return Map.entry(eColumn.getValues(), filtered);
  }

  private Map.Entry<int[], byte[]> computeEnumEnumBlock(byte tableId,
                                                        CProfile cProfile,
                                                        long[] timestamp,
                                                        long blockId,
                                                        BitSet accepted) {
    EColumn eColumn = enumDAO.getEColumnValues(tableId, blockId, cProfile.getColId());
    byte[] eBytes = new byte[timestamp.length];
    IntStream.range(0, timestamp.length)
        .filter(accepted::get)
        .forEach(iR -> eBytes[iR] = eColumn.getDataByte()[iR]);
    return Map.entry(eColumn.getValues(), eBytes);
  }

  private List<String> computeRawBlock(byte tableId,
                                       CProfile cProfile,
                                       long[] timestamp,
                                       long blockId,
                                       BitSet accepted) {
    List<String> columnData = new ArrayList<>();
    String[] columValues = getStringArrayValuesRaw(rawDAO, tableId, blockId, cProfile);
    IntStream.range(0, timestamp.length)
        .filter(accepted::get)
        .forEach(i -> columnData.add(columValues[i]));
    return columnData;
  }

  private <T, V> void setMapValueCommon(Map<T, Map<V, Integer>> map,
                                        List<T> listFirst,
                                        List<V> listSecond,
                                        int sum) {
    for (int i = 0; i < listFirst.size(); i++) {
      setMapValue(map, listFirst.get(i), listSecond.get(i), sum);
    }
  }

  private void setMapValueRawEnumBlock(Map<String, Map<Integer, Integer>> map,
                                       List<String> listFirst,
                                       Map.Entry<int[], byte[]> entrySecond,
                                       int sum) {
    for (int i = 0; i < listFirst.size(); i++) {
      byte enumByte = entrySecond.getValue()[i];        // now filtered
      int intToRaw = EnumHelper.getIndexValue(entrySecond.getKey(), enumByte);
      setMapValueRawEnumBlock(map, listFirst.get(i), intToRaw, sum);
    }
  }

  protected void setMapValueEnumBlock(Map<Integer, Map<Integer, Integer>> map,
                                      Integer vFirst,
                                      int vSecond,
                                      int sum) {
    Map<Integer, Integer> innerMap = map.computeIfAbsent(vFirst, k -> new HashMap<>());
    innerMap.merge(vSecond, sum, Integer::sum);
  }

  protected void setMapValueRawEnumBlock(Map<String, Map<Integer, Integer>> map,
                                         String vFirst,
                                         int vSecond,
                                         int sum) {
    Map<Integer, Integer> innerMap = map.computeIfAbsent(vFirst, k -> new HashMap<>());
    innerMap.merge(vSecond, sum, Integer::sum);
  }

  protected void setMapValueRawEnumBlock(Map<Integer, Map<String, Integer>> map,
                                         int vFirst,
                                         String vSecond,
                                         int sum) {
    Map<String, Integer> innerMap = map.computeIfAbsent(vFirst, k -> new HashMap<>());
    innerMap.merge(vSecond, sum, Integer::sum);
  }

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
    int firstColId = firstGrpBy.getColId();
    int secondColId = secondGrpBy.getColId();

    long previousBlockId = this.rawDAO.getPreviousBlockId(tableId, begin);
    Map.Entry<MetadataKey, MetadataKey> keyEntry = getMetadataKeyPair(tableId, begin, end, previousBlockId);

    Map<String, Double> resultMap = new HashMap<>();

    try (EntityCursor<Metadata> cursor = rawDAO.getMetadataEntityCursor(keyEntry.getKey(), keyEntry.getValue())) {
      Metadata columnKey;

      while ((columnKey = cursor.next()) != null) {
        long blockId = columnKey.getMetadataKey().getBlockId();

        SType firstSType = getSType(firstColId, columnKey);
        SType secondSType = getSType(secondColId, columnKey);

        if (checkSTypeILocal(firstSType, secondSType, SType.HISTOGRAM, SType.HISTOGRAM)) {
          computeHistHistSum(tableId, blockId, firstGrpBy, secondGrpBy, tsColId, begin, end, compositeFilter, resultMap);
        } else if (checkSTypeILocal(firstSType, secondSType, SType.ENUM, SType.ENUM)) {
          computeEnumEnumSum(tableId, tsColId, blockId, firstGrpBy, secondGrpBy, begin, end, compositeFilter, resultMap);
        } else if (checkSTypeILocal(firstSType, secondSType, SType.RAW, SType.RAW)) {
          computeRawRawSum(tableId, firstGrpBy, secondGrpBy, tsColId, blockId, begin, end, compositeFilter, resultMap);
        } else if (checkSTypeILocal(firstSType, secondSType, SType.HISTOGRAM, SType.ENUM)) {
          computeHistEnumSum(tableId, firstGrpBy, secondGrpBy, tsColId, blockId, begin, end, compositeFilter, resultMap);
        } else if (checkSTypeILocal(firstSType, secondSType, SType.ENUM, SType.HISTOGRAM)) {
          computeEnumHistSum(tableId, firstGrpBy, secondGrpBy, tsColId, blockId, begin, end, compositeFilter, resultMap);
        } else if (checkSTypeILocal(firstSType, secondSType, SType.HISTOGRAM, SType.RAW)) {
          computeHistRawSum(tableId, firstGrpBy, secondGrpBy, tsColId, blockId, begin, end, compositeFilter, resultMap);
        } else if (checkSTypeILocal(firstSType, secondSType, SType.RAW, SType.HISTOGRAM)) {
          computeRawHistSum(tableId, firstGrpBy, secondGrpBy, tsColId, blockId, begin, end, compositeFilter, resultMap);
        } else if (checkSTypeILocal(firstSType, secondSType, SType.ENUM, SType.RAW)) {
          computeEnumRawSum(tableId, firstGrpBy, secondGrpBy, tsColId, blockId, begin, end, compositeFilter, resultMap);
        } else if (checkSTypeILocal(firstSType, secondSType, SType.RAW, SType.ENUM)) {
          computeRawEnumSum(tableId, firstGrpBy, secondGrpBy, tsColId, blockId, begin, end, compositeFilter, resultMap);
        }
      }
    } catch (Exception e) {
      log.error("Error processing gantt sum data", e);
    }

    return resultMap.entrySet().stream()
        .map(entry -> new GanttColumnSum(entry.getKey(), entry.getValue()))
        .collect(Collectors.toList());
  }

  private void computeEnumEnumSum(byte tableId,
                                  int tsColId,
                                  long blockId,
                                  CProfile firstGrpBy,
                                  CProfile secondGrpBy,
                                  long begin,
                                  long end,
                                  CompositeFilter compositeFilter,
                                  Map<String, Double> resultMap) {
    long[] timestamp = rawDAO.getRawLong(tableId, blockId, tsColId);
    Map<FilterCondition, String[]> cache = buildFilterCache(compositeFilter, tableId, blockId, timestamp);
    BitSet acc = acceptedRows(timestamp, begin, end, compositeFilter, cache);

    if (acc.isEmpty()) {
      return;
    }

    // Pre-fetch both enum columns once
    EColumn firstCol = enumDAO.getEColumnValues(tableId, blockId, firstGrpBy.getColId());
    EColumn secondCol = enumDAO.getEColumnValues(tableId, blockId, secondGrpBy.getColId());

    int[] firstDict = firstCol.getValues();
    int[] secondDict = secondCol.getValues();
    byte[] firstBytes = firstCol.getDataByte();
    byte[] secondBytes = secondCol.getDataByte();

    // Pre-compute conversions to avoid repeated method calls
    Map<Integer, String> firstValueCache = new HashMap<>();
    Map<Integer, Double> secondValueCache = new HashMap<>();

    // Single pass over accepted rows only
    for (int i = acc.nextSetBit(0); i >= 0; i = acc.nextSetBit(i + 1)) {
      // First column - group key
      byte firstByte = firstBytes[i];
      int firstIntValue = EnumHelper.getIndexValue(firstDict, firstByte);
      String groupKey = firstValueCache.computeIfAbsent(firstIntValue,
                                                        k -> converter.convertIntToRaw(k, firstGrpBy));

      // Second column - sum value
      byte secondByte = secondBytes[i];
      int secondIntValue = EnumHelper.getIndexValue(secondDict, secondByte);
      double value = secondValueCache.computeIfAbsent(secondIntValue,
                                                      k -> converter.convertIntFromDoubleLong(k, secondGrpBy));

      resultMap.merge(groupKey, value, Double::sum);
    }
  }

  private void computeHistEnumSum(byte tableId,
                                  CProfile firstGrpBy,
                                  CProfile secondGrpBy,
                                  int tsColId,
                                  long blockId,
                                  long begin,
                                  long end,
                                  CompositeFilter compositeFilter,
                                  Map<String, Double> resultMap) {
    long[] timestamp = rawDAO.getRawLong(tableId, blockId, tsColId);
    Map<FilterCondition, String[]> cache = buildFilterCache(compositeFilter, tableId, blockId, timestamp);
    BitSet acc = acceptedRows(timestamp, begin, end, compositeFilter, cache);

    if (acc.isEmpty()) {
      return;
    }

    // Pre-fetch data
    int[][] histograms = histogramDAO.get(tableId, blockId, firstGrpBy.getColId());
    int[] unpackedHistogram = getHistogramUnPack(timestamp, histograms);
    double[] secondColumnValues = getDoubleArrayValues(rawDAO, enumDAO, histogramDAO, converter,
                                                       tableId, secondGrpBy, blockId, timestamp);

    // Pre-compute conversions
    Map<Integer, String> firstValueCache = new HashMap<>();

    // Single pass over accepted rows
    for (int i = acc.nextSetBit(0); i >= 0; i = acc.nextSetBit(i + 1)) {
      String groupKey = firstValueCache.computeIfAbsent(unpackedHistogram[i],
                                                        k -> converter.convertIntToRaw(k, firstGrpBy));
      double value = secondColumnValues[i];
      resultMap.merge(groupKey, value, Double::sum);
    }
  }

  private void computeRawRawSum(byte tableId,
                                CProfile firstGrpBy,
                                CProfile secondGrpBy,
                                int tsColId,
                                long blockId,
                                long begin,
                                long end,
                                CompositeFilter compositeFilter,
                                Map<String, Double> resultMap) {
    long[] timestamps = rawDAO.getRawLong(tableId, blockId, tsColId);
    Map<FilterCondition, String[]> cache = buildFilterCache(compositeFilter, tableId, blockId, timestamps);
    BitSet acc = acceptedRows(timestamps, begin, end, compositeFilter, cache);

    if (acc.isEmpty()) {
      return;
    }

    // Pre-fetch data
    String[] firstColumnValues = getStringArrayValuesRaw(rawDAO, tableId, blockId, firstGrpBy);
    double[] secondColumnValues = getDoubleArrayValuesRaw(rawDAO, tableId, blockId, secondGrpBy);

    // Single pass over accepted rows
    for (int i = acc.nextSetBit(0); i >= 0; i = acc.nextSetBit(i + 1)) {
      resultMap.merge(firstColumnValues[i], secondColumnValues[i], Double::sum);
    }
  }

  private void computeHistHistSum(byte tableId,
                                  long blockId,
                                  CProfile firstGrpBy,
                                  CProfile secondGrpBy,
                                  int tsColId,
                                  long begin,
                                  long end,
                                  CompositeFilter compositeFilter,
                                  Map<String, Double> resultMap) {
    long[] timestamps = rawDAO.getRawLong(tableId, blockId, tsColId);
    Map<FilterCondition, String[]> cache = buildFilterCache(compositeFilter, tableId, blockId, timestamps);
    BitSet acc = acceptedRows(timestamps, begin, end, compositeFilter, cache);

    if (acc.isEmpty()) {
      return;
    }

    // Pre-fetch data
    int[][] firstHist = histogramDAO.get(tableId, blockId, firstGrpBy.getColId());
    int[] firstUnpacked = getHistogramUnPack(timestamps, firstHist);
    double[] secondColumnValues = getDoubleArrayValues(rawDAO, enumDAO, histogramDAO, converter,
                                                       tableId, secondGrpBy, blockId, timestamps);

    // Pre-compute conversions
    Map<Integer, String> firstValueCache = new HashMap<>();

    // Single pass over accepted rows
    for (int i = acc.nextSetBit(0); i >= 0; i = acc.nextSetBit(i + 1)) {
      String groupKey = firstValueCache.computeIfAbsent(firstUnpacked[i],
                                                        k -> converter.convertIntToRaw(k, firstGrpBy));
      double value = secondColumnValues[i];
      resultMap.merge(groupKey, value, Double::sum);
    }
  }

  private void computeEnumHistSum(byte tableId,
                                  CProfile firstGrpBy,
                                  CProfile secondGrpBy,
                                  int tsColId,
                                  long blockId,
                                  long begin,
                                  long end,
                                  CompositeFilter compositeFilter,
                                  Map<String, Double> resultMap) {
    long[] timestamp = rawDAO.getRawLong(tableId, blockId, tsColId);
    Map<FilterCondition, String[]> cache = buildFilterCache(compositeFilter, tableId, blockId, timestamp);
    BitSet acc = acceptedRows(timestamp, begin, end, compositeFilter, cache);

    if (acc.isEmpty()) {
      return;
    }

    // Pre-fetch data
    EColumn firstCol = enumDAO.getEColumnValues(tableId, blockId, firstGrpBy.getColId());
    int[] firstDict = firstCol.getValues();
    byte[] firstBytes = firstCol.getDataByte();
    double[] secondColumnValues = getDoubleArrayValues(rawDAO, enumDAO, histogramDAO, converter,
                                                       tableId, secondGrpBy, blockId, timestamp);

    // Pre-compute conversions
    Map<Integer, String> firstValueCache = new HashMap<>();

    // Single pass over accepted rows
    for (int i = acc.nextSetBit(0); i >= 0; i = acc.nextSetBit(i + 1)) {
      int firstIntValue = EnumHelper.getIndexValue(firstDict, firstBytes[i]);
      String groupKey = firstValueCache.computeIfAbsent(firstIntValue,
                                                        k -> converter.convertIntToRaw(k, firstGrpBy));
      double value = secondColumnValues[i];
      resultMap.merge(groupKey, value, Double::sum);
    }
  }

  private void computeHistRawSum(byte tableId,
                                 CProfile firstGrpBy,
                                 CProfile secondGrpBy,
                                 int tsColId,
                                 long blockId,
                                 long begin,
                                 long end,
                                 CompositeFilter compositeFilter,
                                 Map<String, Double> resultMap) {
    long[] timestamps = rawDAO.getRawLong(tableId, blockId, tsColId);
    Map<FilterCondition, String[]> cache = buildFilterCache(compositeFilter, tableId, blockId, timestamps);
    BitSet acc = acceptedRows(timestamps, begin, end, compositeFilter, cache);

    if (acc.isEmpty()) {
      return;
    }

    // Pre-fetch data
    int[][] histograms = histogramDAO.get(tableId, blockId, firstGrpBy.getColId());
    int[] unpackedHistogram = getHistogramUnPack(timestamps, histograms);
    double[] secondColumnValues = getDoubleArrayValuesRaw(rawDAO, tableId, blockId, secondGrpBy);

    // Pre-compute conversions
    Map<Integer, String> firstValueCache = new HashMap<>();

    // Single pass over accepted rows
    for (int i = acc.nextSetBit(0); i >= 0; i = acc.nextSetBit(i + 1)) {
      String groupKey = firstValueCache.computeIfAbsent(unpackedHistogram[i],
                                                        k -> converter.convertIntToRaw(k, firstGrpBy));
      resultMap.merge(groupKey, secondColumnValues[i], Double::sum);
    }
  }

  private void computeRawHistSum(byte tableId,
                                 CProfile firstGrpBy,
                                 CProfile secondGrpBy,
                                 int tsColId,
                                 long blockId,
                                 long begin,
                                 long end,
                                 CompositeFilter compositeFilter,
                                 Map<String, Double> resultMap) {
    long[] timestamps = rawDAO.getRawLong(tableId, blockId, tsColId);
    Map<FilterCondition, String[]> cache = buildFilterCache(compositeFilter, tableId, blockId, timestamps);
    BitSet acc = acceptedRows(timestamps, begin, end, compositeFilter, cache);

    if (acc.isEmpty()) {
      return;
    }

    // Pre-fetch data
    String[] firstColumnValues = getStringArrayValuesRaw(rawDAO, tableId, blockId, firstGrpBy);
    double[] secondColumnValues = getDoubleArrayValues(rawDAO, enumDAO, histogramDAO, converter,
                                                       tableId, secondGrpBy, blockId, timestamps);

    // Single pass over accepted rows
    for (int i = acc.nextSetBit(0); i >= 0; i = acc.nextSetBit(i + 1)) {
      resultMap.merge(firstColumnValues[i], secondColumnValues[i], Double::sum);
    }
  }

  private void computeEnumRawSum(byte tableId,
                                 CProfile firstGrpBy,
                                 CProfile secondGrpBy,
                                 int tsColId,
                                 long blockId,
                                 long begin,
                                 long end,
                                 CompositeFilter compositeFilter,
                                 Map<String, Double> resultMap) {
    long[] timestamps = rawDAO.getRawLong(tableId, blockId, tsColId);
    Map<FilterCondition, String[]> cache = buildFilterCache(compositeFilter, tableId, blockId, timestamps);
    BitSet acc = acceptedRows(timestamps, begin, end, compositeFilter, cache);

    if (acc.isEmpty()) {
      return;
    }

    // Pre-fetch data
    EColumn firstCol = enumDAO.getEColumnValues(tableId, blockId, firstGrpBy.getColId());
    int[] firstDict = firstCol.getValues();
    byte[] firstBytes = firstCol.getDataByte();
    double[] secondColumnValues = getDoubleArrayValuesRaw(rawDAO, tableId, blockId, secondGrpBy);

    // Pre-compute conversions
    Map<Integer, String> firstValueCache = new HashMap<>();

    // Single pass over accepted rows
    for (int i = acc.nextSetBit(0); i >= 0; i = acc.nextSetBit(i + 1)) {
      int firstIntValue = EnumHelper.getIndexValue(firstDict, firstBytes[i]);
      String groupKey = firstValueCache.computeIfAbsent(firstIntValue,
                                                        k -> converter.convertIntToRaw(k, firstGrpBy));
      resultMap.merge(groupKey, secondColumnValues[i], Double::sum);
    }
  }

  private void computeRawEnumSum(byte tableId,
                                 CProfile firstGrpBy,
                                 CProfile secondGrpBy,
                                 int tsColId,
                                 long blockId,
                                 long begin,
                                 long end,
                                 CompositeFilter compositeFilter,
                                 Map<String, Double> resultMap) {
    long[] timestamps = rawDAO.getRawLong(tableId, blockId, tsColId);
    Map<FilterCondition, String[]> cache = buildFilterCache(compositeFilter, tableId, blockId, timestamps);
    BitSet acc = acceptedRows(timestamps, begin, end, compositeFilter, cache);

    if (acc.isEmpty()) {
      return;
    }

    // Pre-fetch data
    String[] firstColumnValues = getStringArrayValuesRaw(rawDAO, tableId, blockId, firstGrpBy);
    double[] secondColumnValues = getDoubleArrayValues(rawDAO, enumDAO, histogramDAO, converter,
                                                       tableId, secondGrpBy, blockId, timestamps);

    // Single pass over accepted rows
    for (int i = acc.nextSetBit(0); i >= 0; i = acc.nextSetBit(i + 1)) {
      resultMap.merge(firstColumnValues[i], secondColumnValues[i], Double::sum);
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