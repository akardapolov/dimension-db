package ru.dimension.db.model.output;

import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder(toBuilder = true)
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class StackedColumn {

  @EqualsAndHashCode.Include
  private long key;

  private long tail;

  @Builder.Default
  private Map<String, Integer> keyCount = new HashMap<>();
  @Builder.Default
  private Map<String, Double> keySum = new HashMap<>();
  @Builder.Default
  private Map<String, Double> keyAvg = new HashMap<>();
  @Builder.Default
  private Map<String, Double> keyPercentile = new HashMap<>();
}
