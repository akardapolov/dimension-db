package ru.dimension.db.service.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import ru.dimension.db.model.profile.cstype.SType;
import ru.dimension.db.service.StatisticsService;

public class StatisticsServiceImpl implements StatisticsService {
  private static final int HISTORY_SIZE = 5;

  // tableId -> colId -> stack of storage types (limited to HISTORY_SIZE)
  private final Map<Byte, Map<Integer, CircularBuffer<StorageType>>> stats = new HashMap<>();
  private final Map<Byte, Integer> lastAnalyzedColIdMap = new HashMap<>();
  private final Map<Byte, Boolean> fullPassDoneMap = new HashMap<>();

  @Override
  public boolean isStatByTableExist(byte tableId) {
    if (stats.get(tableId) == null || stats.get(tableId).isEmpty()) {
      stats.computeIfAbsent(tableId, k -> new HashMap<>());
      return false;
    } else {
      return true;
    }
  }

  @Override
  public SType getLastSType(byte tableId, int colId, boolean isTimestamp) {
    if (isTimestamp) return SType.RAW;

    Map<Integer, CircularBuffer<StorageType>> tableStats = stats.get(tableId);
    if (tableStats == null) return SType.RAW;

    CircularBuffer<StorageType> history = tableStats.get(colId);

    if (history == null || history.isEmpty()) {
      stats.get(tableId)
          .computeIfAbsent(colId, k -> new CircularBuffer<>(HISTORY_SIZE))
          .add(new StorageType(SType.RAW));
      return SType.RAW;
    }

    return history.getLastAdded().sType();
  }
  public Integer getLastAnalyzedColId(byte tableId) {
    return lastAnalyzedColIdMap.get(tableId);
  }

  public void setLastAnalyzedColId(byte tableId, int colId) {
    lastAnalyzedColIdMap.put(tableId, colId);
  }

  public void updateSType(byte tableId, int colId, SType sType) {
    stats.computeIfAbsent(tableId, k -> new HashMap<>())
        .computeIfAbsent(colId, k -> new CircularBuffer<>(HISTORY_SIZE))
        .add(new StorageType(sType));
  }

  public boolean isFullPassDone(byte tableId) {
    return fullPassDoneMap.getOrDefault(tableId, false);
  }

  public void setFullPassDone(byte tableId) {
    fullPassDoneMap.put(tableId, true);
  }

  private record StorageType(SType sType) {}

  private static class CircularBuffer<T> {
    private final LinkedList<T> stack = new LinkedList<>();
    private final int maxSize;

    CircularBuffer(int size) {
      this.maxSize = size;
    }

    void add(T item) {
      stack.addLast(item);
      if (stack.size() > maxSize) {
        stack.removeFirst();
      }
    }

    boolean isEmpty() {
      return stack.isEmpty();
    }

    T getLastAdded() {
      return stack.getLast();
    }
  }
}