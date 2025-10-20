package ru.dimension.db.service;

import static ru.dimension.db.util.MapArrayUtil.parseStringToTypedArray;
import static ru.dimension.db.util.MapArrayUtil.parseStringToTypedMap;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.log4j.Log4j2;
import ru.dimension.db.metadata.DataType;
import ru.dimension.db.model.filter.CompositeFilter;
import ru.dimension.db.model.filter.FilterCondition;
import ru.dimension.db.model.filter.LogicalOperator;
import ru.dimension.db.model.output.GanttColumnCount;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.cstype.CType;
import ru.dimension.db.model.profile.cstype.SType;
import ru.dimension.db.service.mapping.Mapper;
import ru.dimension.db.service.store.HEntry;
import ru.dimension.db.storage.Converter;
import ru.dimension.db.storage.EnumDAO;
import ru.dimension.db.storage.HistogramDAO;
import ru.dimension.db.storage.RawDAO;
import ru.dimension.db.storage.bdb.entity.Metadata;
import ru.dimension.db.storage.bdb.entity.MetadataKey;
import ru.dimension.db.storage.bdb.entity.column.EColumn;
import ru.dimension.db.storage.helper.EnumHelper;
import ru.dimension.db.util.CachedLastLinkedHashMap;
import ru.dimension.db.util.DateHelper;

@Log4j2
public abstract class CommonServiceApi {

  public Predicate<CProfile> isNotTimestamp = Predicate.not(f -> f.getCsType().isTimeStamp());
  public Predicate<CProfile> isRaw = Predicate.not(f -> f.getCsType().getSType() != SType.RAW);
  public Predicate<CProfile> isInt = Predicate.not(f -> Mapper.isCType(f) != CType.INT);
  public Predicate<CProfile> isLong = Predicate.not(f -> Mapper.isCType(f) != CType.LONG);
  public Predicate<CProfile> isFloat = Predicate.not(f -> Mapper.isCType(f) != CType.FLOAT);
  public Predicate<CProfile> isDouble = Predicate.not(f -> Mapper.isCType(f) != CType.DOUBLE);
  public Predicate<CProfile> isString = Predicate.not(f -> Mapper.isCType(f) != CType.STRING);

  public static <T> List<List<T>> transpose(List<List<T>> table) {
    if (table.isEmpty()) {
      return new ArrayList<>();
    }

    int rowCount = table.size();
    int maxCol = 0;
    for (List<T> row : table) {
      if (row.size() > maxCol) {
        maxCol = row.size();
      }
    }

    List<List<T>> ret = new ArrayList<>(maxCol);
    for (int j = 0; j < maxCol; j++) {
      List<T> newRow = new ArrayList<>(rowCount);
      for (int i = 0; i < rowCount; i++) {
        List<T> row = table.get(i);
        T element = j < row.size() ? row.get(j) : null;
        newRow.add(element);
      }
      ret.add(newRow);
    }
    return ret;
  }

  protected int getHistogramValue(int iR, int[][] histogram) {
    int[] indices = histogram[0];
    int[] values = histogram[1];
    int n = indices.length;
    int low = 0, high = n - 1, result = -1;
    while (low <= high) {
      int mid = (low + high) >>> 1;
      if (indices[mid] <= iR) {
        result = mid;
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }
    return (result == -1) ? 0 : values[result];
  }

  public int[][] getArrayFromMapHEntry(HEntry hEntry) {
    return new int[][]{hEntry.getIndices(), hEntry.getValues()};
  }

  protected <T, V> void setMapValue(Map<T, Map<V, Integer>> map,
                                    T vFirst,
                                    V vSecond,
                                    int sum) {
    map.computeIfAbsent(vFirst, k -> new HashMap<>())
        .merge(vSecond, sum, Integer::sum);
  }

  protected long[][] getArrayLong(List<List<Long>> rawDataLong) {
    int size = rawDataLong.size();
    long[][] result = new long[size][];
    for (int i = 0; i < size; i++) {
      List<Long> row = rawDataLong.get(i);
      long[] arr = new long[row.size()];
      for (int j = 0; j < row.size(); j++) arr[j] = row.get(j);
      result[i] = arr;
    }
    return result;
  }

  protected double[][] getArrayDouble(List<List<Double>> rawDataDouble) {
    int size = rawDataDouble.size();
    double[][] result = new double[size][];
    for (int i = 0; i < size; i++) {
      List<Double> row = rawDataDouble.get(i);
      double[] arr = new double[row.size()];
      for (int j = 0; j < row.size(); j++) arr[j] = row.get(j);
      result[i] = arr;
    }
    return result;
  }

  protected String[][] getArrayString(List<List<String>> rawDataString) {
    int size = rawDataString.size();
    String[][] array = new String[size][];
    for (int i = 0; i < size; i++) {
      List<String> row = rawDataString.get(i);
      array[i] = row.toArray(new String[0]);
    }
    return array;
  }

  public byte[] getByteFromList(List<Byte> list) {
    byte[] byteArray = new byte[list.size()];
    for (int i = 0; i < list.size(); i++) byteArray[i] = list.get(i);
    return byteArray;
  }

  public int[] getIntegerFromSet(Set<Integer> set) {
    int[] values = new int[set.size()];
    int index = 0;
    for (Integer key : set) values[index++] = key;
    return values;
  }

  public void fillTimestampMap(List<CProfile> cProfiles,
                               CachedLastLinkedHashMap<Integer, Integer> mapping) {
    int index = 0;
    for (CProfile profile : cProfiles) {
      if (profile.getCsType().isTimeStamp()) {
        mapping.put(profile.getColId(), index++);
      }
    }
  }

  public void fillAllEnumMappingSType(List<CProfile> cProfiles,
                                      CachedLastLinkedHashMap<Integer, Integer> mapping,
                                      List<CachedLastLinkedHashMap<Integer, Byte>> rawDataEnumEColumn,
                                      Map<Integer, SType> colIdSTypeMap) {
    int index = 0;
    for (CProfile profile : cProfiles) {
      if (!profile.getCsType().isTimeStamp()
          && SType.ENUM.equals(colIdSTypeMap.get(profile.getColId()))) {
        mapping.put(profile.getColId(), index);
        rawDataEnumEColumn.add(index, new CachedLastLinkedHashMap<>());
        index++;
      }
    }
  }

  public void fillMappingRaw(List<CProfile> cProfiles,
                             CachedLastLinkedHashMap<Integer, Integer> mapping,
                             Map<Integer, SType> colIdSTypeMap,
                             Predicate<CProfile> isNotTimestamp,
                             Predicate<CProfile> isCustom) {
    int index = 0;
    for (CProfile profile : cProfiles) {
      if (isNotTimestamp.test(profile)
          && isCustom.test(profile)
          && SType.RAW.equals(colIdSTypeMap.get(profile.getColId()))) {
        mapping.put(profile.getColId(), index++);
      }
    }
  }

  public void fillMappingRaw(List<CProfile> cProfiles,
                             List<Integer> mapping,
                             Predicate<CProfile> isRaw,
                             Predicate<CProfile> isCustom) {
    int index = 0;
    for (CProfile profile : cProfiles) {
      if (isRaw.test(profile) && isCustom.test(profile)) {
        mapping.add(index++, profile.getColId());
      }
    }
  }

  protected String[] getStringArrayValues(RawDAO rawDAO,
                                          EnumDAO enumDAO,
                                          HistogramDAO histogramDAO,
                                          Converter converter,
                                          byte tableId,
                                          CProfile cProfile,
                                          long blockId,
                                          long[] timestamps) {
    MetadataKey metadataKey = MetadataKey.builder()
        .tableId(tableId)
        .blockId(blockId)
        .build();
    SType sType = getSType(cProfile.getColId(), rawDAO.getMetadata(metadataKey));
    return switch (sType) {
      case RAW -> getStringArrayValuesRaw(rawDAO, tableId, blockId, cProfile);
      case ENUM -> getStringArrayValuesEnum(enumDAO, converter, tableId, blockId, cProfile, timestamps);
      case HISTOGRAM -> getStringArrayValuesHist(histogramDAO, converter, tableId, blockId, cProfile, timestamps);
    };
  }

  public String[] getStringArrayValuesRaw(RawDAO rawDAO,
                                          byte tableId,
                                          long blockId,
                                          CProfile cProfile) {
    int colId = cProfile.getColId();
    CType cType = Mapper.isCType(cProfile);
    switch (cType) {
      case INT -> {
        int[] intVals = rawDAO.getRawInt(tableId, blockId, colId);
        String[] resI = new String[intVals.length];
        for (int i = 0; i < intVals.length; i++) {
          resI[i] = (intVals[i] == Mapper.INT_NULL) ? "" : String.valueOf(intVals[i]);
        }
        return resI;
      }
      case LONG -> {
        long[] longVals = rawDAO.getRawLong(tableId, blockId, colId);
        String[] resL = new String[longVals.length];
        for (int i = 0; i < longVals.length; i++) {
          if (longVals[i] == Mapper.LONG_NULL) {
            resL[i] = "";
          } else if (isDateTimeType(cProfile.getCsType().getDType())) {
            resL[i] = getDateForLongShorted(Math.toIntExact(longVals[i] / 1000));
          } else {
            resL[i] = String.valueOf(longVals[i]);
          }
        }
        return resL;
      }
      case FLOAT -> {
        float[] floatVals = rawDAO.getRawFloat(tableId, blockId, colId);
        String[] resF = new String[floatVals.length];
        for (int i = 0; i < floatVals.length; i++) {
          resF[i] = (floatVals[i] == Mapper.FLOAT_NULL) ? "" : String.valueOf(floatVals[i]);
        }
        return resF;
      }
      case DOUBLE -> {
        double[] doubleVals = rawDAO.getRawDouble(tableId, blockId, colId);
        String[] resD = new String[doubleVals.length];
        for (int i = 0; i < doubleVals.length; i++) {
          resD[i] = (doubleVals[i] == Mapper.DOUBLE_NULL) ? "" : String.valueOf(doubleVals[i]);
        }
        return resD;
      }
      case STRING -> {
        String[] strVals = rawDAO.getRawString(tableId, blockId, colId);
        for (int i = 0; i < strVals.length; i++) {
          if (strVals[i] == null)
            strVals[i] = "";
        }
        return strVals;
      }
      default -> {
        return new String[0];
      }
    }
  }

  protected String[] getStringArrayValuesEnum(EnumDAO enumDAO,
                                              Converter converter,
                                              byte tableId,
                                              long blockId,
                                              CProfile cProfile,
                                              long[] timestamps) {
    String[] array = new String[timestamps.length];
    EColumn eColumn = enumDAO.getEColumnValues(tableId, blockId, cProfile.getColId());
    IntStream.range(0, timestamps.length).forEach(iR -> {
      byte valueByte = eColumn.getDataByte()[iR];
      array[iR] = converter.convertIntToRaw(EnumHelper.getIndexValue(eColumn.getValues(), valueByte), cProfile);
    });
    return array;
  }

  protected String[] getStringArrayValuesHist(HistogramDAO histogramDAO,
                                              Converter converter,
                                              byte tableId,
                                              long blockId,
                                              CProfile cProfile,
                                              long[] timestamps) {
    String[] array = new String[timestamps.length];
    int[][] histograms = histogramDAO.get(tableId, blockId, cProfile.getColId());
    int[] unpacked = getHistogramUnPack(timestamps, histograms);
    for (int i = 0; i < timestamps.length; i++) {
      array[i] = converter.convertIntToRaw(unpacked[i], cProfile);
    }
    return array;
  }

  protected double[] getDoubleArrayValues(RawDAO rawDAO,
                                          EnumDAO enumDAO,
                                          HistogramDAO histogramDAO,
                                          Converter converter,
                                          byte tableId,
                                          CProfile cProfile,
                                          long blockId,
                                          long[] timestamps) {
    MetadataKey metadataKey = MetadataKey.builder()
        .tableId(tableId)
        .blockId(blockId)
        .build();
    SType sType = getSType(cProfile.getColId(), rawDAO.getMetadata(metadataKey));
    return switch (sType) {
      case RAW -> getDoubleArrayValuesRaw(rawDAO, tableId, blockId, cProfile);
      case ENUM -> getDoubleArrayValuesEnum(enumDAO, converter, tableId, blockId, cProfile, timestamps);
      case HISTOGRAM -> getDoubleArrayValuesHist(histogramDAO, converter, tableId, blockId, cProfile, timestamps);
    };
  }

  protected double[] getDoubleArrayValuesRaw(RawDAO rawDAO,
                                             byte tableId,
                                             long blockId,
                                             CProfile cProfile) {
    int colId = cProfile.getColId();
    CType cType = Mapper.isCType(cProfile);
    switch (cType) {
      case INT -> {
        int[] vals = rawDAO.getRawInt(tableId, blockId, colId);
        double[] res = new double[vals.length];
        for (int i = 0; i < vals.length; i++)
          res[i] = (vals[i] == Mapper.INT_NULL) ? 0 : vals[i];
        return res;
      }
      case LONG -> {
        long[] vals = rawDAO.getRawLong(tableId, blockId, colId);
        double[] res = new double[vals.length];
        for (int i = 0; i < vals.length; i++)
          res[i] = (vals[i] == Mapper.LONG_NULL) ? 0 : vals[i];
        return res;
      }
      case FLOAT -> {
        float[] vals = rawDAO.getRawFloat(tableId, blockId, colId);
        double[] res = new double[vals.length];
        for (int i = 0; i < vals.length; i++)
          res[i] = (vals[i] == Mapper.FLOAT_NULL) ? 0 : vals[i];
        return res;
      }
      case DOUBLE -> {
        double[] vals = rawDAO.getRawDouble(tableId, blockId, colId);
        double[] res = new double[vals.length];
        for (int i = 0; i < vals.length; i++)
          res[i] = (vals[i] == Mapper.DOUBLE_NULL) ? 0 : vals[i];
        return res;
      }
      default -> {
        return new double[0];
      }
    }
  }

  protected double[] getDoubleArrayValuesEnum(EnumDAO enumDAO,
                                              Converter converter,
                                              byte tableId,
                                              long blockId,
                                              CProfile cProfile,
                                              long[] timestamps) {
    double[] array = new double[timestamps.length];
    EColumn eColumn = enumDAO.getEColumnValues(tableId, blockId, cProfile.getColId());
    IntStream.range(0, timestamps.length).forEach(iR -> {
      byte valueByte = eColumn.getDataByte()[iR];
      int intValue = EnumHelper.getIndexValue(eColumn.getValues(), valueByte);
      array[iR] = converter.convertIntFromDoubleLong(intValue, cProfile);
    });
    return array;
  }

  protected double[] getDoubleArrayValuesHist(HistogramDAO histogramDAO,
                                              Converter converter,
                                              byte tableId,
                                              long blockId,
                                              CProfile cProfile,
                                              long[] timestamps) {
    double[] array = new double[timestamps.length];
    int[][] histograms = histogramDAO.get(tableId, blockId, cProfile.getColId());
    int[] unpacked = getHistogramUnPack(timestamps, histograms);
    for (int i = 0; i < timestamps.length; i++) {
      array[i] = converter.convertIntFromDoubleLong(unpacked[i], cProfile);
    }
    return array;
  }

  protected <V, T> void fillColumnData(V[] columValues,
                                       long[] timestamps,
                                       long begin,
                                       long end,
                                       List<T> columnData) {
    if (columValues.length == 0) return;
    for (int i = 0; i < timestamps.length; i++) {
      if (timestamps[i] >= begin && timestamps[i] <= end) {
        columnData.add((T) columValues[i]);
      }
    }
  }

  protected SType getSType(int colId, Metadata metadata) {
    for (int id : metadata.getRawColIds())      if (id == colId) return SType.RAW;
    for (int id : metadata.getEnumColIds())     if (id == colId) return SType.ENUM;
    for (int id : metadata.getHistogramColIds())if (id == colId) return SType.HISTOGRAM;
    throw new RuntimeException("Undefined storage type for column id: " + colId);
  }

  protected Map.Entry<MetadataKey, MetadataKey> getMetadataKeyPair(byte tableId,
                                                                   long begin,
                                                                   long end,
                                                                   long previousBlockId) {
    MetadataKey beginMKey;
    if (previousBlockId != begin && previousBlockId != 0) {
      beginMKey = MetadataKey.builder().tableId(tableId).blockId(previousBlockId).build();
    } else {
      beginMKey = MetadataKey.builder().tableId(tableId).blockId(begin).build();
    }
    MetadataKey endMKey = MetadataKey.builder().tableId(tableId).blockId(end).build();
    return new SimpleImmutableEntry<>(beginMKey, endMKey);
  }

  protected int getNextIndex(int i, int[][] histogram, long[] timestamps) {
    return (i + 1 < histogram[0].length)
        ? histogram[0][i + 1] - 1
        : timestamps.length - 1;
  }

  private boolean isDateTimeType(DataType type) {
    return type == DataType.TIMESTAMP ||
        type == DataType.TIMESTAMPTZ ||
        type == DataType.DATETIME ||
        type == DataType.DATETIME2 ||
        type == DataType.SMALLDATETIME;
  }

  private String getDateForLongShorted(int longDate) {
    return DateHelper.format(longDate);
  }

  protected LocalDateTime toLocalDateTime(long epochMilli) {
    return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMilli),
                                   TimeZone.getDefault().toZoneId());
  }

  protected int[] getHistogramUnPack(long[] timestamps,
                                     int[][] histograms) {
    int n = timestamps.length;
    if (n == 0) {
      return new int[0];
    }

    int[] indices = histograms[0];
    int[] values = histograms[1];
    int[] unpacked = new int[n];

    if (indices.length == 0) {
      return unpacked;
    }

    int currentIndex = 0;
    int currentValue = values[0];

    for (int i = 0; i < n; i++) {
      if (currentIndex + 1 < indices.length && i >= indices[currentIndex + 1]) {
        currentIndex++;
        currentValue = values[currentIndex];
      }
      unpacked[i] = currentValue;
    }

    return unpacked;
  }

  protected List<GanttColumnCount> handleMap(CProfile firstLevelGroupBy,
                                             CProfile secondLevelGroupBy,
                                             Map<String, Map<String, Integer>> mapFinalIn,
                                             CompositeFilter compositeFilter) {
    List<GanttColumnCount> list = new ArrayList<>();
    Map<String, Map<String, Integer>> mapFinalOut = new HashMap<>();

    if (DataType.MAP.equals(firstLevelGroupBy.getCsType().getDType())) {
      handlerFirstLevelMap(mapFinalIn, mapFinalOut, firstLevelGroupBy, compositeFilter);
    }

    if (DataType.MAP.equals(secondLevelGroupBy.getCsType().getDType())) {
      if (DataType.MAP.equals(firstLevelGroupBy.getCsType().getDType())) {
        Map<String, Map<String, Integer>> updates = new HashMap<>();
        handlerSecondLevelMap(mapFinalOut, updates, secondLevelGroupBy, compositeFilter);
        mapFinalOut.clear();
        updates.forEach((key, val) ->
                            val.forEach((updateKey, updateValue) -> setMapValue(mapFinalOut, key, updateKey, updateValue))
        );
      } else {
        handlerSecondLevelMap(mapFinalIn, mapFinalOut, secondLevelGroupBy, compositeFilter);
      }
    }

    for (Map.Entry<String, Map<String, Integer>> entry : mapFinalOut.entrySet()) {
      list.add(GanttColumnCount.builder().key(entry.getKey()).gantt(entry.getValue()).build());
    }

    return list;
  }

  private void handlerFirstLevelMap(Map<String, Map<String, Integer>> mapFinalIn,
                                    Map<String, Map<String, Integer>> mapFinalOut,
                                    CProfile cProfile,
                                    CompositeFilter compositeFilter) {
    Set<String> filterKeys = getFilterDataForProfile(cProfile, compositeFilter);

    for (Map.Entry<String, Map<String, Integer>> entry : mapFinalIn.entrySet()) {
      Map<String, Long> parsedMap = parsedMap(entry.getKey());
      if (parsedMap.isEmpty()) {
        for (Map.Entry<String, Integer> innerEntry : entry.getValue().entrySet()) {
          setMapValue(mapFinalOut, Mapper.STRING_NULL, innerEntry.getKey(), innerEntry.getValue());
        }
      } else {
        for (Map.Entry<String, Long> parsedEntry : parsedMap.entrySet()) {
          String mapKey = parsedEntry.getKey();
          if (filterKeys == null || filterKeys.contains(mapKey)) {
            for (Map.Entry<String, Integer> innerEntry : entry.getValue().entrySet()) {
              setMapValue(mapFinalOut, mapKey, innerEntry.getKey(),
                          Math.toIntExact(parsedEntry.getValue()) * innerEntry.getValue());
            }
          }
        }
      }
    }
  }

  private void handlerSecondLevelMap(Map<String, Map<String, Integer>> mapFinalIn,
                                     Map<String, Map<String, Integer>> mapFinalOut,
                                     CProfile cProfile,
                                     CompositeFilter compositeFilter) {
    Set<String> filterKeys = getFilterDataForProfile(cProfile, compositeFilter);
    for (Map.Entry<String, Map<String, Integer>> entry : mapFinalIn.entrySet()) {
      for (Map.Entry<String, Integer> innerEntry : entry.getValue().entrySet()) {
        Map<String, Long> parsedMap = parsedMap(innerEntry.getKey());
        if (parsedMap.isEmpty()) {
          setMapValue(mapFinalOut, entry.getKey(), Mapper.STRING_NULL, innerEntry.getValue());
        } else {
          for (Map.Entry<String, Long> parsedEntry : parsedMap.entrySet()) {
            String mapKey = parsedEntry.getKey();
            if (filterKeys == null || filterKeys.contains(mapKey)) {
              setMapValue(mapFinalOut, entry.getKey(), mapKey,
                          Math.toIntExact(parsedEntry.getValue()) * innerEntry.getValue());
            }
          }
        }
      }
    }
  }

  private Map<String, Long> parsedMap(String input) {
    return parseStringToTypedMap(
        input,
        String::new,
        Long::parseLong,
        "="
    );
  }

  protected <T> void fillArrayList(List<List<T>> array,
                                   int colCount) {
    for (int i = 0; i < colCount; i++) {
      array.add(new ArrayList<>());
    }
  }

  protected <T> void fillTimeSeriesColumnData(long[] timestamps, long begin, long end, List<T> columnData) {
    if (timestamps.length == 0) return;

    for (int i = 0; i < timestamps.length; i++) {
      long ts = timestamps[i];
      if (ts >= begin && ts <= end) {
        columnData.add((T) Long.valueOf(ts));
      }
    }
  }

  protected Map<String, Map<String, Integer>> handleArray(CProfile firstLevelGroupBy,
                                                          CProfile secondLevelGroupBy,
                                                          Map<String, Map<String, Integer>> mapFinalIn,
                                                          CompositeFilter compositeFilter) {
    Map<String, Map<String, Integer>> mapFinalOut = new HashMap<>();

    if (DataType.ARRAY.equals(firstLevelGroupBy.getCsType().getDType())) {
      handlerFirstLevelArray(mapFinalIn, mapFinalOut, firstLevelGroupBy, compositeFilter);
    }

    if (DataType.ARRAY.equals(secondLevelGroupBy.getCsType().getDType())) {
      if (DataType.ARRAY.equals(firstLevelGroupBy.getCsType().getDType())) {
        Map<String, Map<String, Integer>> updates = new HashMap<>();
        handlerSecondLevelArray(mapFinalOut, updates, secondLevelGroupBy, compositeFilter);
        mapFinalOut.clear();
        updates.forEach((key, val) ->
                            val.forEach((updateKey, updateValue) -> setMapValue(mapFinalOut, key, updateKey, updateValue))
        );
      } else {
        handlerSecondLevelArray(mapFinalIn, mapFinalOut, secondLevelGroupBy, compositeFilter);
      }
    }

    return mapFinalOut;
  }

  protected <F, T> void fillTimeSeriesColumnDataFilter(Set<F> indexSetFilter, long[] timestamps, List<T> columnData) {
    if (timestamps.length == 0) return;

    for (int i = 0; i < timestamps.length; i++) {
      if (indexSetFilter.contains(i)) {
        columnData.add((T) Long.valueOf(timestamps[i]));
      }
    }
  }

  protected <V, F, T> void fillColumnDataFilter(V[] columValues, Set<F> indexSetFilter, long[] timestamps, List<T> columnData) {
    if (columValues.length == 0) return;

    for (int i = 0; i < timestamps.length; i++) {
      if (indexSetFilter.contains(i)) {
        columnData.add((T) columValues[i]);
      }
    }
  }

  protected <I, V> Set<I> getIndexSetByFilter(V[] columValues,
                                              CProfile cProfileFilter,
                                              String filter,
                                              long[] timestamps,
                                              long begin,
                                              long end) {
    if (cProfileFilter.getCsType().isTimeStamp()) {
      throw new RuntimeException("Not supported for timestamp");
    }

    Set<I> setIndexRow = new HashSet<>();
    if (timestamps.length == 0) return setIndexRow;

    for (int i = 0; i < timestamps.length; i++) {
      long ts = timestamps[i];
      if (ts >= begin && ts <= end && filter.equals(columValues[i])) {
        setIndexRow.add((I) Integer.valueOf(i));
      }
    }

    return setIndexRow;
  }

  private void handlerFirstLevelArray(Map<String, Map<String, Integer>> mapFinalIn,
                                      Map<String, Map<String, Integer>> mapFinalOut,
                                      CProfile cProfile,
                                      CompositeFilter compositeFilter) {
    Set<String> filterElements = getFilterDataForProfile(cProfile, compositeFilter);
    for (Map.Entry<String, Map<String, Integer>> entry : mapFinalIn.entrySet()) {
      String[] array = parseStringToTypedArray(entry.getKey(), ",");
      if (array.length == 0) {
        for (Map.Entry<String, Integer> innerEntry : entry.getValue().entrySet()) {
          setMapValue(mapFinalOut, Mapper.STRING_NULL, innerEntry.getKey(), innerEntry.getValue());
        }
      } else {
        for (String element : array) {
          String trimmed = element.trim();
          if (filterElements == null || filterElements.contains(trimmed)) {
            for (Map.Entry<String, Integer> innerEntry : entry.getValue().entrySet()) {
              setMapValue(mapFinalOut, trimmed, innerEntry.getKey(), innerEntry.getValue());
            }
          }
        }
      }
    }
  }

  private void handlerSecondLevelArray(Map<String, Map<String, Integer>> mapFinalIn,
                                       Map<String, Map<String, Integer>> mapFinalOut,
                                       CProfile cProfile,
                                       CompositeFilter compositeFilter) {
    Set<String> filterElements = getFilterDataForProfile(cProfile, compositeFilter);
    for (Map.Entry<String, Map<String, Integer>> entry : mapFinalIn.entrySet()) {
      for (Map.Entry<String, Integer> innerEntry : entry.getValue().entrySet()) {
        String[] array = parseStringToTypedArray(innerEntry.getKey(), ",");
        if (array.length == 0) {
          setMapValue(mapFinalOut, entry.getKey(), Mapper.STRING_NULL, innerEntry.getValue());
        } else {
          for (String element : array) {
            String trimmed = element.trim();
            if (filterElements == null || filterElements.contains(trimmed)) {
              setMapValue(mapFinalOut, entry.getKey(), trimmed, innerEntry.getValue());
            }
          }
        }
      }
    }
  }

  protected BitSet acceptedRows(long[] timestamps,
                                long begin,
                                long end,
                                CompositeFilter filter,
                                Map<FilterCondition, String[]> cache) {
    BitSet bs = new BitSet(timestamps.length);
    for (int i = 0; i < timestamps.length; i++) {
      long ts = timestamps[i];
      if (ts >= begin && ts <= end) {
        if (filter == null || filter.getConditions().isEmpty()) {
          bs.set(i);
          continue;
        }

        boolean rowAccepted = true;
        if (filter.getOperator() == LogicalOperator.AND) {
          // AND: all conditions must be true
          for (FilterCondition c : filter.getConditions()) {
            String[] values = cache.get(c);
            if (values == null || i >= values.length) {
              rowAccepted = false;
              break;
            }
            String v = values[i];
            v = formatFloatingPoint(v, c.getCProfile().getCsType().getCType());
            if (!valueMatchesCondition(v, c)) {
              rowAccepted = false;
              break;
            }
          }
        } else {
          // OR: at least one condition must be true
          rowAccepted = false;
          for (FilterCondition c : filter.getConditions()) {
            String[] values = cache.get(c);
            if (values != null && i < values.length) {
              String v = values[i];
              v = formatFloatingPoint(v, c.getCProfile().getCsType().getCType());
              if (valueMatchesCondition(v, c)) {
                rowAccepted = true;
                break;
              }
            }
          }
        }

        if (rowAccepted) {
          bs.set(i);
        }
      }
    }
    return bs;
  }

  private String formatFloatingPoint(String value, CType cType) {
    if (cType != CType.FLOAT && cType != CType.DOUBLE) return value;
    try {
      double num = Double.parseDouble(value);
      return String.format("%.1f", num).replace(",", ".");
    } catch (NumberFormatException e) {
      return value;
    }
  }

  protected boolean checkSTypeILocal(SType first, SType second, SType firstCompare, SType secondCompare) {
    return first.equals(firstCompare) && second.equals(secondCompare);
  }

  protected boolean evaluateRowFilter(CompositeFilter filter, int rowIndex, Map<CProfile, String[]> filterValuesMap) {
    if (filter == null || filter.getConditions().isEmpty()) {
      return true;
    }

    List<Boolean> results = new ArrayList<>();
    for (FilterCondition condition : filter.getConditions()) {
      String[] values = filterValuesMap.get(condition.getCProfile());
      if (values == null || rowIndex >= values.length) {
        results.add(false);
        continue;
      }
      String rawValue = values[rowIndex];
      results.add(valueMatchesCondition(rawValue, condition));
    }

    if (filter.getOperator() == LogicalOperator.AND) {
      return results.stream().allMatch(Boolean::booleanValue);
    } else {
      return results.stream().anyMatch(Boolean::booleanValue);
    }
  }

  protected boolean valueMatchesCondition(String rawValue, FilterCondition condition) {
    DataType dataType = condition.getCProfile().getCsType().getDType();

    if (DataType.MAP.equals(dataType)) {
      return mapValueMatches(rawValue, condition.getFilterData());
    } else if (DataType.ARRAY.equals(dataType)) {
      return arrayValueMatches(rawValue, condition.getFilterData());
    } else {
      return scalarValueMatches(rawValue, condition);
    }
  }

  protected boolean scalarValueMatches(String rawValue, FilterCondition condition) {
    if (rawValue == null) {
      return false;
    }
    String[] filterData = condition.getFilterData();
    if (filterData == null || filterData.length == 0) {
      return true;
    }

    switch (condition.getCompareFunction()) {
      case EQUAL:
        for (String filter : filterData) {
          if (filter != null && filter.equals(rawValue)) {
            return true;
          }
        }
        return false;
      case CONTAIN:
        String lowerValue = rawValue.toLowerCase();
        for (String filter : filterData) {
          if (filter != null && lowerValue.contains(filter.toLowerCase())) {
            return true;
          }
        }
        return false;
      default:
        return false;
    }
  }

  protected boolean mapValueMatches(String rawValue, String[] filterKeys) {
    if (rawValue == null || rawValue.isEmpty() || filterKeys == null || filterKeys.length == 0) {
      return false;
    }
    try {
      Map<String, Long> parsedMap = parseStringToTypedMap(rawValue, String::new, Long::parseLong, "=");
      for (String key : filterKeys) {
        if (key != null && parsedMap.containsKey(key.trim())) {
          return true;
        }
      }
    } catch (Exception e) {
      log.warn("Failed to parse map for filtering: '{}'", rawValue, e);
      // Fallback: check if raw string contains any filter key
      for (String key : filterKeys) {
        if (key != null && rawValue.contains(key.trim())) {
          return true;
        }
      }
    }
    return false;
  }

  protected boolean arrayValueMatches(String rawValue, String[] filterElements) {
    if (rawValue == null || rawValue.isEmpty() || filterElements == null || filterElements.length == 0) {
      return false;
    }
    try {
      String[] array = parseStringToTypedArray(rawValue, ",");
      Set<String> elements = Arrays.stream(array)
          .map(String::trim)
          .collect(Collectors.toSet());
      for (String filter : filterElements) {
        if (filter != null && elements.contains(filter.trim())) {
          return true;
        }
      }
    } catch (Exception e) {
      log.warn("Failed to parse array for filtering: '{}'", rawValue, e);
      // Fallback: check if raw string contains any filter element
      for (String filter : filterElements) {
        if (filter != null && rawValue.contains(filter.trim())) {
          return true;
        }
      }
    }
    return false;
  }

  protected void processValueByDataType(String value, Map<String, Integer> valueCounts, CProfile cProfile, CompositeFilter compositeFilter) {
    DataType dataType = cProfile.getCsType().getDType();

    if (DataType.MAP.equals(dataType)) {
      processMapValue(value, valueCounts, cProfile, compositeFilter);
    } else if (DataType.ARRAY.equals(dataType)) {
      processArrayValue(value, valueCounts, cProfile, compositeFilter);
    } else {
      processScalarValue(value, valueCounts);
    }
  }

  protected void processMapValue(String value, Map<String, Integer> valueCounts, CProfile cProfile, CompositeFilter compositeFilter) {
    try {
      Map<String, Long> parsedMap = parseStringToTypedMap(
          value,
          String::new,
          Long::parseLong,
          "="
      );

      Set<String> filterKeys = getFilterDataForProfile(cProfile, compositeFilter);

      for (Map.Entry<String, Long> mapEntry : parsedMap.entrySet()) {
        String mapKey = mapEntry.getKey();
        if (filterKeys == null || filterKeys.contains(mapKey)) {
          long multiplier = mapEntry.getValue() != null ? mapEntry.getValue() : 1;
          valueCounts.merge(mapKey, (int) multiplier, Integer::sum);
        }
      }
    } catch (Exception e) {
      log.warn("Failed to parse map value: '{}' for column {}", value, cProfile.getColName(), e);
      if(getFilterDataForProfile(cProfile, compositeFilter) == null) {
        valueCounts.merge(value, 1, Integer::sum);
      }
    }
  }

  protected void processArrayValue(String value, Map<String, Integer> valueCounts, CProfile cProfile, CompositeFilter compositeFilter) {
    try {
      String[] array = parseStringToTypedArray(value, ",");
      Set<String> filterElements = getFilterDataForProfile(cProfile, compositeFilter);

      for (String element : array) {
        String trimmedElement = element.trim();
        if (!trimmedElement.isEmpty()) {
          if (filterElements == null || filterElements.contains(trimmedElement)) {
            valueCounts.merge(trimmedElement, 1, Integer::sum);
          }
        }
      }
    } catch (Exception e) {
      log.warn("Failed to parse array value: '{}' for column {}", value, cProfile.getColName(), e);
      if (getFilterDataForProfile(cProfile, compositeFilter) == null) {
        valueCounts.merge(value, 1, Integer::sum);
      }
    }
  }

  protected Set<String> getFilterDataForProfile(CProfile cProfile, CompositeFilter compositeFilter) {
    if (compositeFilter == null) {
      return null;
    }
    return compositeFilter.getConditions().stream()
        .filter(c -> c.getCProfile().equals(cProfile))
        .findFirst()
        .map(c -> new LinkedHashSet<>(Arrays.asList(c.getFilterData())))
        .orElse(null);
  }

  protected void processScalarValue(String value, Map<String, Integer> valueCounts) {
    valueCounts.compute(value, (k, count) -> count == null ? 1 : count + 1);
  }
}