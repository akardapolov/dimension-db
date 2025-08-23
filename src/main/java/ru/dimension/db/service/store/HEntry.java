package ru.dimension.db.service.store;

import java.util.Arrays;
import ru.dimension.db.service.mapping.Mapper;

public class HEntry {
  private int[] indices;
  private int[] values;
  private int size;

  public HEntry() {
    this(16);
  }

  public HEntry(int initialCapacity) {
    this.indices = new int[initialCapacity];
    this.values = new int[initialCapacity];
    this.size = 0;
  }

  public void add(int index, int value) {
    ensureCapacity(size + 1);
    indices[size] = index;
    values[size] = value;
    size++;
  }

  private void ensureCapacity(int minCapacity) {
    if (minCapacity > indices.length) {
      int newCapacity = Math.max(indices.length * 2, minCapacity);
      indices = Arrays.copyOf(indices, newCapacity);
      values = Arrays.copyOf(values, newCapacity);
    }
  }

  public int[] getIndices() {
    return Arrays.copyOf(indices, size);
  }

  public int[] getValues() {
    return Arrays.copyOf(values, size);
  }

  public int getValueAtRow(int rowIndex) {
    int pos = Arrays.binarySearch(indices, 0, size, rowIndex);
    if (pos >= 0) {
      return values[pos];
    } else {
      int insertionPoint = -pos - 1;
      if (insertionPoint == 0) {
        return values[0];
      }
      return values[insertionPoint - 1];
    }
  }

  public int getSize() {
    return size;
  }

  public int getLastValue() {
    if (size == 0) {
      return Mapper.INT_NULL;
    }
    return values[size - 1];
  }
}