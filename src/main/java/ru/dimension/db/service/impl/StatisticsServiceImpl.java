package ru.dimension.db.service.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import ru.dimension.db.model.profile.cstype.SType;
import ru.dimension.db.service.StatisticsService;

public class StatisticsServiceImpl implements StatisticsService {
  private static final int HISTORY_SIZE = 5;

  // tableId -> colId -> stack of storage types (limited to HISTORY_SIZE)
  private final Map<Byte, Map<Integer, STypeCircularBuffer>> stats = new ConcurrentHashMap<>();
  private final Map<Byte, Integer> lastAnalyzedColIdMap = new ConcurrentHashMap<>();
  private final Map<Byte, Boolean> fullPassDoneMap = new ConcurrentHashMap<>();

  @Override
  public boolean isStatByTableExist(byte tableId) {
    if (stats.get(tableId) == null || stats.get(tableId).isEmpty()) {
      stats.computeIfAbsent(tableId, k -> new ConcurrentHashMap<>());
      return false;
    } else {
      return true;
    }
  }

  @Override
  public SType getLastSType(byte tableId, int colId, boolean isTimestamp) {
    if (isTimestamp) return SType.RAW;

    Map<Integer, STypeCircularBuffer> tableStats = stats.get(tableId);
    if (tableStats == null) return SType.RAW;

    STypeCircularBuffer history = tableStats.get(colId);

    if (history == null || history.isEmpty()) {
      stats.get(tableId)
          .computeIfAbsent(colId, k -> new STypeCircularBuffer(HISTORY_SIZE))
          .add(SType.RAW);
      return SType.RAW;
    }

    return history.getLastAdded();
  }
  public Integer getLastAnalyzedColId(byte tableId) {
    return lastAnalyzedColIdMap.get(tableId);
  }

  public void setLastAnalyzedColId(byte tableId, int colId) {
    lastAnalyzedColIdMap.put(tableId, colId);
  }

  public void updateSType(byte tableId, int colId, SType sType) {
    stats.computeIfAbsent(tableId, k -> new ConcurrentHashMap<>())
        .computeIfAbsent(colId, k -> new STypeCircularBuffer(HISTORY_SIZE))
        .add(sType);
  }

  public boolean isFullPassDone(byte tableId) {
    return fullPassDoneMap.getOrDefault(tableId, false);
  }

  public void setFullPassDone(byte tableId) {
    fullPassDoneMap.put(tableId, true);
  }

  private static class STypeCircularBuffer {
    private final byte[] buffer;
    private int head;
    private int tail;
    private int count;
    private final int capacity;

    STypeCircularBuffer(int capacity) {
      this.capacity = capacity;
      this.buffer = new byte[capacity];
      this.head = 0;
      this.tail = 0;
      this.count = 0;
    }

    void add(SType sType) {
      buffer[tail] = sType.getKey();
      tail = (tail + 1) % capacity;

      if (count < capacity) {
        count++;
      } else {
        head = (head + 1) % capacity;
      }
    }

    SType getLastAdded() {
      if (count == 0) throw new IllegalStateException("Buffer is empty");
      int lastIndex = (tail - 1 + capacity) % capacity;
      return SType.values()[buffer[lastIndex] - 1];
    }

    boolean isEmpty() {
      return count == 0;
    }
  }
}