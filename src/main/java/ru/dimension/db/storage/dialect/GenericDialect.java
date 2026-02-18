package ru.dimension.db.storage.dialect;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import ru.dimension.db.model.CompareFunction;
import ru.dimension.db.model.GroupFunction;
import ru.dimension.db.model.OrderBy;
import ru.dimension.db.model.filter.CompositeFilter;
import ru.dimension.db.model.filter.FilterCondition;
import ru.dimension.db.model.profile.CProfile;

import java.util.ArrayList;
import java.util.List;
import ru.dimension.db.model.filter.LogicalOperator;

public class GenericDialect implements DatabaseDialect {

  @Override
  public String getSelectClassGantt(CProfile firstCProfile, CProfile secondCProfile) {
    String firstColName = firstCProfile.getColName();
    String secondColName = secondCProfile.getColName();
    return "SELECT " + firstColName + ", " + secondColName + ", COUNT(*) AS value ";
  }

  @Override
  public String getSelectClassStacked(GroupFunction groupFunction, CProfile cProfile) {
    String colName = cProfile.getColName();
    if (GroupFunction.COUNT.equals(groupFunction)) {
      return "SELECT " + colName + ", COUNT(*) AS value ";
    } else if (GroupFunction.SUM.equals(groupFunction)) {
      return "SELECT " + colName + ", SUM(" + colName + ") AS value ";
    } else if (GroupFunction.AVG.equals(groupFunction)) {
      return "SELECT " + colName + ", AVG(" + colName + ") AS value ";
    } else {
      throw new RuntimeException("Unsupported group function: " + groupFunction);
    }
  }

  @Override
  public String getWhereClass(CProfile tsCProfile, CProfile cProfileFilter,
                              String[] filterData, CompareFunction compareFunction) {
    StringBuilder whereClause = new StringBuilder("WHERE ");
    whereClause.append(tsCProfile.getColName()).append(" BETWEEN ? AND ?");

    if (cProfileFilter != null && filterData != null && filterData.length > 0) {
      whereClause.append(getFilterAndString(cProfileFilter, filterData, compareFunction));
    }

    return whereClause.toString();
  }

  @Override
  public String getOrderByClass(CProfile tsCProfile) {
    return " ORDER BY " + tsCProfile.getColName();
  }

  @Override
  public String getOrderByClass(CProfile cProfile, OrderBy orderBy) {
    return " ORDER BY " + cProfile.getColName() + " " + orderBy.name();
  }

  @Override
  public String getLimitClass(Integer fetchSize) {
    return fetchSize != null ? " LIMIT " + fetchSize : "";
  }

  @Override
  public String getOffsetClass(int offset) {
    return " OFFSET " + offset;
  }

  @Override
  public void setDateTime(CProfile tsCProfile, PreparedStatement ps,
                          int parameterIndex, long unixTimestamp) throws SQLException {
    ps.setTimestamp(parameterIndex, new Timestamp(unixTimestamp));
  }

  @Override
  public String getWhereClassWithCompositeFilter(CProfile tsCProfile, CompositeFilter compositeFilter) {
    StringBuilder whereClause = new StringBuilder("WHERE ");
    whereClause.append(tsCProfile.getColName()).append(" BETWEEN ? AND ?");

    if (compositeFilter != null && !compositeFilter.getConditions().isEmpty()) {
      whereClause.append(" AND (");

      List<String> conditions = new ArrayList<>();
      for (FilterCondition condition : compositeFilter.getConditions()) {
        String filterStr = getFilterConditionString(condition);
        if (!filterStr.isEmpty()) {
          conditions.add(filterStr);
        }
      }

      if (!conditions.isEmpty()) {
        String joinOperator = compositeFilter.getOperator() == LogicalOperator.AND ? " AND " : " OR ";
        whereClause.append(String.join(joinOperator, conditions));
      }

      whereClause.append(")");
    }

    return whereClause.toString();
  }

  @Override
  public String getWhereClassWithCompositeFilterNoTimestamp(CompositeFilter compositeFilter) {
    if (compositeFilter == null || compositeFilter.getConditions().isEmpty()) {
      return "";
    }

    StringBuilder whereClause = new StringBuilder();
    whereClause.append("WHERE (");

    List<String> conditions = new ArrayList<>();
    for (FilterCondition condition : compositeFilter.getConditions()) {
      String filterStr = getFilterConditionString(condition);
      if (!filterStr.isEmpty()) {
        conditions.add(filterStr);
      }
    }

    if (conditions.isEmpty()) {
      return "";
    }

    String joinOperator = compositeFilter.getOperator() == LogicalOperator.AND ? " AND " : " OR ";
    whereClause.append(String.join(joinOperator, conditions))
        .append(")");

    return whereClause.toString();
  }

  private String getFilterConditionString(FilterCondition condition) {
    return "(" + getFilterAndStringRaw(
        condition.getCProfile(),
        condition.getFilterData(),
        condition.getCompareFunction()
    ) + ")";
  }

  private String getFilterAndStringRaw(CProfile cProfileFilter, String[] filterData, CompareFunction compareFunction) {
    if (cProfileFilter == null || filterData == null) {
      return "";
    }

    String columnName = cProfileFilter.getColName();
    StringBuilder filterClause = new StringBuilder();
    boolean isNumeric = isNumericType(cProfileFilter);

    for (String filterValue : filterData) {
      String condition;
      if (filterValue == null || filterValue.trim().isEmpty()) {
        if (isNumeric) {
          condition = columnName + " IS NULL";
        } else {
          condition = columnName + " IS NULL OR " + columnName + " = ''";
        }
      } else {
        String formattedValue = filterValue.trim();
        if (CompareFunction.CONTAIN.equals(compareFunction)) {
          formattedValue = "%" + formattedValue.toLowerCase() + "%";
          condition = "LOWER(" + columnName + ") LIKE '" + formattedValue.replace("'", "''") + "'";
        } else {
          condition = columnName + " = '" + formattedValue.replace("'", "''") + "'";
        }
      }
      if (filterClause.length() > 0) {
        filterClause.append(" OR ");
      }
      filterClause.append(condition);
    }

    return filterClause.toString();
  }

  private String getFilterAndString(CProfile cProfileFilter, String[] filterData, CompareFunction compareFunction) {
    String raw = getFilterAndStringRaw(cProfileFilter, filterData, compareFunction);
    return raw.isEmpty() ? "" : " AND (" + raw + ")";
  }
}