package ru.dimension.db.model.filter;

import lombok.Data;
import ru.dimension.db.model.CompareFunction;
import ru.dimension.db.model.profile.CProfile;

@Data
public class FilterCondition {
  private final CProfile cProfile;
  private final String[] filterData;
  private final CompareFunction compareFunction;

  public FilterCondition(CProfile cProfile, String[] filterData, CompareFunction compareFunction) {
    this.cProfile = cProfile;
    this.filterData = filterData;
    this.compareFunction = compareFunction;
  }
}