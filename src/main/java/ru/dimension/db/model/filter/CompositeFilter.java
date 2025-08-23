package ru.dimension.db.model.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import lombok.Getter;
import ru.dimension.db.model.CompareFunction;

@Getter
public class CompositeFilter {
  private final List<FilterCondition> conditions;
  private final LogicalOperator operator;
  private final transient Predicate<String[]> combinedPredicate;

  public CompositeFilter(List<FilterCondition> conditions, LogicalOperator operator) {
    this.conditions = conditions;
    this.operator = operator;
    this.combinedPredicate = createCombinedPredicate();
  }

  private Predicate<String[]> createCombinedPredicate() {
    if (conditions == null || conditions.isEmpty()) {
      return values -> true; // No filtering
    }

    List<Predicate<String[]>> predicates = new ArrayList<>();
    for (int i = 0; i < conditions.size(); i++) {
      final int index = i;
      FilterCondition condition = conditions.get(i);
      predicates.add(values -> {
        if (values == null || index >= values.length) {
          return false;
        }

        String value = values[index];
        return filter(value, condition.getFilterData(), condition.getCompareFunction());
      });
    }

    if (operator == LogicalOperator.AND) {
      return predicates.stream().reduce(Predicate::and).orElse(values -> true);
    } else {
      return predicates.stream().reduce(Predicate::or).orElse(values -> true);
    }
  }

  public boolean test(String[] values) {
    if (values == null) {
      return conditions == null || conditions.isEmpty();
    }
    return combinedPredicate.test(values);
  }

  private boolean filter(String filterValue, String[] filterData, CompareFunction compareFunction) {
    if (filterData == null || filterData.length == 0) {
      return true;
    }

    if (filterValue == null) {
      return false;
    }

    switch (compareFunction) {
      case EQUAL -> {
        for (String filter : filterData) {
          if (filter != null && filter.equals(filterValue)) {
            return true;
          }
        }
        return false;
      }
      case CONTAIN -> {
        String lowerValue = filterValue.toLowerCase();
        for (String filter : filterData) {
          if (filter != null && lowerValue.contains(filter.toLowerCase())) {
            return true;
          }
        }
        return false;
      }
      default -> throw new IllegalArgumentException("Unsupported CompareFunction: " + compareFunction);
    }
  }
}