package ru.dimension.db.service.store;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import ru.dimension.db.exception.EnumByteExceedException;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.cstype.CType;
import ru.dimension.db.model.profile.cstype.SType;
import ru.dimension.db.service.CommonServiceApi;
import ru.dimension.db.service.mapping.Mapper;
import ru.dimension.db.storage.Converter;
import ru.dimension.db.storage.helper.EnumHelper;
import ru.dimension.db.util.CachedLastLinkedHashMap;

@Getter
@EqualsAndHashCode(callSuper = true)
public class UStore extends CommonServiceApi {

  private final Converter converter;
  private final Map<Integer, List<Object>> rawDataMap;
  private final Map<Integer, List<Byte>> enumDataMap;
  private final Map<Integer, HEntry> histogramDataMap;
  private final Map<Integer, SType> storageTypeMap;
  private final Map<Integer, CachedLastLinkedHashMap<Integer, Byte>> enumDictionaries;

  private final Map<Integer, LFUCache> conversionCacheMap;

  public UStore(Converter converter) {
    this(converter, null);
  }

  public UStore(Converter converter, Map<Integer, LFUCache> conversionCacheMap) {
    this.converter = converter;
    this.rawDataMap = new HashMap<>();
    this.enumDataMap = new HashMap<>();
    this.histogramDataMap = new HashMap<>();
    this.storageTypeMap = new HashMap<>();
    this.enumDictionaries = new CachedLastLinkedHashMap<>();
    this.conversionCacheMap = conversionCacheMap;
  }

  public void addColumn(CProfile cProfile, SType sType) {
    int colId = cProfile.getColId();
    if (storageTypeMap.containsKey(colId)) {
      throw new IllegalArgumentException("Column already exists: " + colId);
    }

    storageTypeMap.put(colId, sType);
    if (sType == SType.RAW) {
      rawDataMap.put(colId, new ArrayList<>());
    } else if (sType == SType.ENUM) {
      enumDataMap.put(colId, new ArrayList<>());
      enumDictionaries.put(colId, new CachedLastLinkedHashMap<>());
    } else if (sType == SType.HISTOGRAM) {
      histogramDataMap.put(colId, new HEntry());
    }
  }

  public void removeColumn(CProfile cProfile) {
    int colId = cProfile.getColId();
    SType sType = storageTypeMap.remove(colId);

    if (sType == SType.RAW) {
      rawDataMap.remove(colId);
    } else if (sType == SType.ENUM) {
      enumDictionaries.remove(colId);
      enumDataMap.remove(colId);
    } else if (sType == SType.HISTOGRAM) {
      histogramDataMap.remove(colId);
    }
  }

  public void add(CProfile cProfile, int iR, Object currObject) {
    int colId = cProfile.getColId();
    SType sType = storageTypeMap.get(colId);
    CType cType = Mapper.isCType(cProfile);

    if (sType == SType.RAW) {
      Object value = convertRawValue(currObject, cProfile, cType);
      insertRawValue(colId, iR, value);
    } else if (sType == SType.ENUM) {
      int currValue = getCachedOrConvert(colId, currObject, cProfile);
      insertEnumValue(colId, iR, currValue, cProfile);
    } else if (sType == SType.HISTOGRAM) {
      int currValue = getCachedOrConvert(colId, currObject, cProfile);
      insertHistogramValue(colId, iR, currValue);
    }
  }

  private int getCachedOrConvert(int colId, Object currObject, CProfile cProfile) {
    if (currObject == null) {
      return Mapper.INT_NULL;
    }

    if (conversionCacheMap == null) {
      return converter.convertRawToInt(currObject, cProfile);
    }

    LFUCache cache = conversionCacheMap.computeIfAbsent(
        colId,
        k -> new LFUCache(100)
    );

    return cache.computeIfAbsent(
        currObject,
        k -> converter.convertRawToInt(k, cProfile)
    );
  }

  private void insertRawValue(int colId, int iR, Object value) {
    List<Object> columnData = rawDataMap.get(colId);
    if (columnData == null) {
      throw new IllegalArgumentException("RAW column not initialized: " + colId);
    }
    columnData.add(iR, value);
  }

  private void insertEnumValue(int colId, int iR, int value, CProfile cProfile) {
    List<Byte> columnData = enumDataMap.get(colId);
    CachedLastLinkedHashMap<Integer, Byte> dictionary = enumDictionaries.get(colId);

    if (columnData == null || dictionary == null) {
      throw new IllegalArgumentException("ENUM column not initialized: " + colId);
    }

    try {
      byte byteValue = EnumHelper.getByteValue(dictionary, value);
      columnData.add(iR, byteValue);
    } catch (EnumByteExceedException e) {
      Map<Byte, Integer> reverseMap = new HashMap<>();
      for (Map.Entry<Integer, Byte> entry : dictionary.entrySet()) {
        reverseMap.put(entry.getValue(), entry.getKey());
      }

      List<Object> rawData = new ArrayList<>();
      for (byte b : columnData) {
        Integer intValue = reverseMap.get(b);
        if (intValue == null) {
          throw new IllegalStateException("Enum value not found for byte: " + b);
        }
        rawData.add(convertIntToObject(value, cProfile));
      }
      rawData.add(iR, convertIntToObject(value, cProfile));

      enumDataMap.remove(colId);
      enumDictionaries.remove(colId);
      storageTypeMap.put(colId, SType.RAW);
      rawDataMap.put(colId, rawData);
    }
  }

  private Object convertRawValue(Object currObject,
                                 CProfile cProfile,
                                 CType cType) {
    switch (cType) {
      case INT: return Mapper.convertRawToInt(currObject, cProfile);
      case LONG: return Mapper.convertRawToLong(currObject, cProfile);
      case FLOAT: return Mapper.convertRawToFloat(currObject, cProfile);
      case DOUBLE: return Mapper.convertRawToDouble(currObject, cProfile);
      case STRING: return Mapper.convertRawToString(currObject, cProfile);
      default: throw new IllegalArgumentException("Unsupported type: " + cType);
    }
  }

  private void insertHistogramValue(int colId, int iR, int value) {
    HEntry entry = histogramDataMap.get(colId);
    if (entry == null) {
      throw new IllegalArgumentException("HISTOGRAM column not initialized: " + colId);
    }

    int prevValue = (entry.getSize() > 0) ? entry.getLastValue() : Mapper.INT_NULL;
    if (prevValue != value || entry.getSize() == 0) {
      entry.add(iR, value);
    }
  }

  public void analyzeAndConvertColumns(int totalRowCount, Map<Integer, CProfile> cProfileMap) {
    List<ColumnConversion> columnsToConvert = new ArrayList<>();

    for (Map.Entry<Integer, SType> entry : storageTypeMap.entrySet()) {
      int colId = entry.getKey();
      SType currentType = entry.getValue();
      CProfile cProfile = cProfileMap.get(colId);
      if (cProfile == null) continue;

      if (cProfile.getCsType().getCType() != CType.INT) {
        continue;
      }

      CType cType = Mapper.isCType(cProfile);
      List<Integer> intValues = getIntegerListForColumn(colId, currentType, totalRowCount, cProfile);
      if (intValues == null || intValues.size() != totalRowCount) continue;

      Set<Integer> distinct = new HashSet<>(intValues);
      int ndv = distinct.size();
      int runs = 1;
      for (int i = 1; i < intValues.size(); i++) {
        if (!intValues.get(i).equals(intValues.get(i - 1))) {
          runs++;
        }
      }

      double clusteringIndex;
      if (totalRowCount == ndv) {
        clusteringIndex = 0.0;
      } else {
        clusteringIndex = (totalRowCount - runs) / (double) (totalRowCount - ndv);
      }

      SType newType;
      if (ndv > 0.5 * totalRowCount) {
        newType = SType.RAW;
      } else {
        newType = clusteringIndex >= 0.8 ? SType.HISTOGRAM : SType.ENUM;
      }

      if (newType != currentType) {
        columnsToConvert.add(new ColumnConversion(colId, cProfile, newType, intValues, cType));
      }
    }

    for (ColumnConversion conv : columnsToConvert) {
      removeColumn(conv.cProfile);
      convertToNewStorage(conv.colId, conv.cProfile, conv.newType, conv.intValues, conv.cType);
    }
  }

  List<Integer> getIntegerListForColumn(int colId, SType currentType, int totalRowCount, CProfile cProfile) {
    if (cProfile.getCsType().getCType() != CType.INT) {
      return null;
    }

    if (currentType == SType.RAW) {
      List<Object> rawData = rawDataMap.get(colId);
      if (rawData == null) return null;
      List<Integer> intValues = new ArrayList<>();
      for (Object obj : rawData) {
        intValues.add(converter.convertRawToInt(obj, cProfile));
      }
      return intValues;
    } else if (currentType == SType.ENUM) {
      List<Byte> enumData = enumDataMap.get(colId);
      CachedLastLinkedHashMap<Integer, Byte> dictionary = enumDictionaries.get(colId);
      Map<Byte, Integer> reverseMap = new HashMap<>();
      for (Map.Entry<Integer, Byte> e : dictionary.entrySet()) {
        reverseMap.put(e.getValue(), e.getKey());
      }
      List<Integer> intValues = new ArrayList<>();
      for (byte b : enumData) {
        Integer value = reverseMap.get(b);
        if (value == null) return null;
        intValues.add(value);
      }
      return intValues;
    } else if (currentType == SType.HISTOGRAM) {
      HEntry entry = histogramDataMap.get(colId);
      int[] indices = entry.getIndices();
      int[] values = entry.getValues();
      List<Integer> intValues = new ArrayList<>(totalRowCount);
      int currentPtr = 0;
      int currentVal = Mapper.INT_NULL;
      for (int r = 0; r < totalRowCount; r++) {
        if (currentPtr < indices.length && r == indices[currentPtr]) {
          currentVal = values[currentPtr];
          currentPtr++;
        }
        intValues.add(currentVal);
      }
      return intValues;
    }
    return null;
  }

  private void convertToNewStorage(int colId, CProfile cProfile, SType newType, List<Integer> intValues, CType cType) {
    removeColumn(cProfile);
    addColumn(cProfile, newType);

    if (newType == SType.RAW) {
      List<Object> rawData = new ArrayList<>();
      for (int value : intValues) {
        if (cType == CType.STRING) {
          rawData.add(converter.convertIntToRaw(value, cProfile));
        } else {
          rawData.add(convertIntToObject(value, cProfile));
        }
      }
      rawDataMap.put(colId, rawData);
    } else if (newType == SType.ENUM) {
      List<Byte> enumData = enumDataMap.get(colId);
      CachedLastLinkedHashMap<Integer, Byte> dictionary = enumDictionaries.get(colId);

      dictionary.clear();
      byte nextByte = 0;

      try {
        for (int value : intValues) {
          if (!dictionary.containsKey(value)) {
            if (nextByte == Byte.MAX_VALUE) {
              throw new EnumByteExceedException("Byte limit exceeded");
            }
            dictionary.put(value, nextByte++);
          }

          byte byteValue = dictionary.get(value);
          enumData.add(byteValue);
        }
      } catch (EnumByteExceedException e) {
        removeColumn(cProfile);
        addColumn(cProfile, SType.RAW);
        List<Object> rawData = new ArrayList<>();
        for (int v : intValues) {
          rawData.add(convertIntToObject(v, cProfile));
        }
        rawDataMap.put(colId, rawData);
      }
    } else if (newType == SType.HISTOGRAM) {
      HEntry entry = histogramDataMap.get(colId);
      int prevValue = Mapper.INT_NULL;
      for (int r = 0; r < intValues.size(); r++) {
        int currValue = intValues.get(r);
        if (r == 0 || currValue != prevValue) {
          entry.add(r, currValue);
          prevValue = currValue;
        }
      }
    }
  }

  public SType analyzeColumn(int colId, int totalRowCount, CProfile cProfile) {
    if (cProfile.getCsType().getCType() != CType.INT) {
      return storageTypeMap.get(colId);
    }

    SType currentType = storageTypeMap.get(colId);

    if (totalRowCount <= 100) {
      List<Integer> intValues = getIntegerListForColumn(colId, currentType, totalRowCount, cProfile);
      if (intValues == null || intValues.size() != totalRowCount) {
        return currentType;
      }
      return computeStorageType(intValues, totalRowCount);
    }

    int sampleSize;
    if (totalRowCount <= 1000) {
      sampleSize = (int) Math.ceil(totalRowCount * 0.2);
    } else if (totalRowCount <= 10000) {
      sampleSize = (int) Math.ceil(totalRowCount * 0.1);
    } else {
      sampleSize = (int) Math.ceil(totalRowCount * 0.01);
    }

    int step = Math.max(1, totalRowCount / sampleSize);
    List<Integer> sampledValues = new ArrayList<>();

    try {
      if (currentType == SType.RAW) {
        List<Object> rawData = rawDataMap.get(colId);
        for (int i = 0; i < totalRowCount; i += step) {
          Object obj = rawData.get(i);
          int value = converter.convertRawToInt(obj, cProfile);
          sampledValues.add(value);
        }
      } else if (currentType == SType.ENUM) {
        List<Byte> enumData = enumDataMap.get(colId);
        CachedLastLinkedHashMap<Integer, Byte> dictionary = enumDictionaries.get(colId);
        Map<Byte, Integer> reverseMap = new HashMap<>();
        for (Map.Entry<Integer, Byte> e : dictionary.entrySet()) {
          reverseMap.put(e.getValue(), e.getKey());
        }
        for (int i = 0; i < totalRowCount; i += step) {
          byte b = enumData.get(i);
          Integer value = reverseMap.get(b);
          sampledValues.add(value != null ? value : 0);
        }
      } else if (currentType == SType.HISTOGRAM) {
        HEntry entry = histogramDataMap.get(colId);
        for (int i = 0; i < totalRowCount; i += step) {
          sampledValues.add(entry.getValueAtRow(i));
        }
      } else {
        return currentType;
      }
    } catch (Exception e) {
      return currentType;
    }

    return computeStorageType(sampledValues, sampledValues.size());
  }

  private SType computeStorageType(List<Integer> values, int sampleSize) {
    if (values.isEmpty()) {
      return SType.RAW;
    }

    Set<Integer> distinct = new HashSet<>(values);
    int ndv = distinct.size();

    int runs = 1;
    for (int i = 1; i < values.size(); i++) {
      if (!values.get(i).equals(values.get(i - 1))) {
        runs++;
      }
    }

    double clusteringIndex;
    if (sampleSize == ndv) {
      clusteringIndex = 0.0;
    } else {
      clusteringIndex = (sampleSize - runs) / (double) (sampleSize - ndv);
    }

    if (ndv > 0.5 * sampleSize) {
      return SType.RAW;
    } else {
      return clusteringIndex >= 0.8 ? SType.HISTOGRAM : SType.ENUM;
    }
  }

  private Object convertIntToObject(int value, CProfile cProfile) {
    CType cType = Mapper.isCType(cProfile);

    return switch (cType) {
      case LONG -> converter.convertIntToLong(value, cProfile);
      case FLOAT -> (float) converter.convertIntToDouble(value, cProfile);
      case DOUBLE -> converter.convertIntToDouble(value, cProfile);
      case STRING -> converter.convertRawToInt(value, cProfile);
      default -> value;
    };
  }

  public static class LFUCache {
    private final int capacity;
    private final Map<Object, Node> cache;
    private final Map<Integer, DoublyLinkedList> freqMap;

    public LFUCache(int capacity) {
      this.capacity = capacity;
      this.cache = new HashMap<>();
      this.freqMap = new HashMap<>();
    }

    public synchronized Integer get(Object key) {
      Node node = cache.get(key);
      if (node == null) {
        return null;
      }
      updateNode(node);
      return node.value;
    }

    public synchronized Integer computeIfAbsent(Object key, Function<Object, Integer> mappingFunction) {
      Integer value = get(key);
      if (value == null) {
        value = mappingFunction.apply(key);
        put(key, value);
      }
      return value;
    }

    public synchronized Integer put(Object key, Integer value) {
      if (capacity <= 0) {
        return null;
      }
      if (cache.containsKey(key)) {
        Node node = cache.get(key);
        node.value = value;
        updateNode(node);
        return value;
      }
      if (cache.size() >= capacity) {
        evict();
      }
      Node node = new Node(key, value, 1);
      cache.put(key, node);
      DoublyLinkedList list = freqMap.computeIfAbsent(1, k -> new DoublyLinkedList());
      list.add(node);
      return value;
    }

    private void updateNode(Node node) {
      int oldFreq = node.freq;
      DoublyLinkedList oldList = freqMap.get(oldFreq);
      if (oldList != null) {
        oldList.remove(node);
        if (oldList.size == 0) {
          freqMap.remove(oldFreq);
        }
      }
      node.freq = oldFreq + 1;
      DoublyLinkedList newList = freqMap.computeIfAbsent(node.freq, k -> new DoublyLinkedList());
      newList.add(node);
    }

    private void evict() {
      if (cache.size() < capacity) {
        return;
      }
      int minFreq = Integer.MAX_VALUE;
      for (int freq : freqMap.keySet()) {
        DoublyLinkedList list = freqMap.get(freq);
        if (list != null && list.size > 0) {
          if (freq < minFreq) {
            minFreq = freq;
          }
        }
      }
      if (minFreq == Integer.MAX_VALUE) {
        return;
      }
      DoublyLinkedList list = freqMap.get(minFreq);
      Node toRemove = list.removeLast();
      if (toRemove != null) {
        cache.remove(toRemove.key);
        if (list.size == 0) {
          freqMap.remove(minFreq);
        }
      }
    }

    private static class Node {
      Object key;
      Integer value;
      int freq;
      Node prev;
      Node next;

      Node() {
      }

      Node(Object key, Integer value, int freq) {
        this.key = key;
        this.value = value;
        this.freq = freq;
      }
    }

    private static class DoublyLinkedList {
      Node head;
      Node tail;
      int size;

      DoublyLinkedList() {
        head = new Node();
        tail = new Node();
        head.next = tail;
        tail.prev = head;
        size = 0;
      }

      void add(Node node) {
        node.next = head.next;
        node.prev = head;
        head.next.prev = node;
        head.next = node;
        size++;
      }

      void remove(Node node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
        size--;
      }

      Node removeLast() {
        if (size == 0) {
          return null;
        }
        Node last = tail.prev;
        remove(last);
        return last;
      }
    }
  }

  private static class ColumnConversion {
    final int colId;
    final CProfile cProfile;
    final SType newType;
    final List<Integer> intValues;
    final CType cType;

    ColumnConversion(int colId, CProfile cProfile, SType newType, List<Integer> intValues, CType cType) {
      this.colId = colId;
      this.cProfile = cProfile;
      this.newType = newType;
      this.intValues = intValues;
      this.cType = cType;
    }
  }
}