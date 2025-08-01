package ru.dimension.db.service.store;

import java.util.Collections;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class HEntry {
  List<Integer> index;
  List<Integer> value;

  public int getValueAtRow(int rowIndex) {
    int pos = Collections.binarySearch(index, rowIndex);
    if (pos >= 0) {
      return value.get(pos);
    } else {
      int insertionPoint = -pos - 1;
      if (insertionPoint == 0) {
        return value.get(0);
      }
      return value.get(insertionPoint - 1);
    }
  }
}
