package ru.dimension.db.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import lombok.extern.log4j.Log4j2;
import ru.dimension.db.core.metamodel.MetaModelApi;
import ru.dimension.db.exception.SqlColMetadataException;
import ru.dimension.db.metadata.DataType;
import ru.dimension.db.model.GroupFunction;
import ru.dimension.db.model.filter.CompositeFilter;
import ru.dimension.db.model.filter.FilterCondition;
import ru.dimension.db.model.output.StackedColumn;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.table.BType;
import ru.dimension.db.service.CommonServiceApi;
import ru.dimension.db.service.GroupByOneService;
import ru.dimension.db.storage.Converter;
import ru.dimension.db.storage.EnumDAO;
import ru.dimension.db.storage.HistogramDAO;
import ru.dimension.db.storage.RawDAO;

@Log4j2
public class GroupByOneServiceImpl extends CommonServiceApi implements GroupByOneService {

  private final MetaModelApi metaModelApi;
  private final Converter converter;
  private final HistogramDAO histogramDAO;
  private final RawDAO rawDAO;
  private final EnumDAO enumDAO;

  public GroupByOneServiceImpl(MetaModelApi metaModelApi,
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
}