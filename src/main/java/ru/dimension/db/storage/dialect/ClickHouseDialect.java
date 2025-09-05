package ru.dimension.db.storage.dialect;

import static ru.dimension.db.storage.helper.ClickHouseHelper.enumParser;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Map;
import ru.dimension.db.metadata.DataType;
import ru.dimension.db.model.CompareFunction;
import ru.dimension.db.model.GroupFunction;
import ru.dimension.db.model.OrderBy;
import ru.dimension.db.model.filter.CompositeFilter;
import ru.dimension.db.model.profile.CProfile;

import java.util.ArrayList;
import java.util.List;
import ru.dimension.db.model.filter.FilterCondition;
import ru.dimension.db.model.filter.LogicalOperator;

public class ClickHouseDialect implements DatabaseDialect {

  @Override
  public String getSelectClassGantt(CProfile firstCProfile,
                                    CProfile secondCProfile) {
    String firstColName = firstCProfile.getColName().toLowerCase();
    String secondColName = secondCProfile.getColName().toLowerCase();

    return "SELECT " + firstColName + ", " + secondColName + ", COUNT(" + secondColName + ") ";
  }

  @Override
  public String getSelectClassStacked(GroupFunction groupFunction, CProfile tsCProfile) {
    String colName = tsCProfile.getColName().toLowerCase();

    if (GroupFunction.COUNT.equals(groupFunction)) {
      return "SELECT " + colName + ", COUNT(" + colName + ") ";
    } else if (GroupFunction.SUM.equals(groupFunction)) {
      return "SELECT '" + colName + "', SUM(" + colName + ") ";
    } else if (GroupFunction.AVG.equals(groupFunction)) {
      return "SELECT '" + colName + "', AVG(" + colName + ") ";
    } else {
      throw new RuntimeException("Not supported");
    }
  }

  @Override
  public String getWhereClass(CProfile tsCProfile,
                              CProfile cProfileFilter,
                              String[] filterData,
                              CompareFunction compareFunction) {
    DataType dataType = tsCProfile.getCsType().getDType();

    if (DataType.DATE.equals(dataType) ||
        DataType.TIMESTAMP.equals(dataType) ||
        DataType.TIMESTAMPTZ.equals(dataType) ||
        DataType.DATETIME2.equals(dataType) ||
        DataType.SMALLDATETIME.equals(dataType)) {
      return "WHERE " + tsCProfile.getColName().toLowerCase()
          + " BETWEEN ? AND ? " + getFilterAndString(cProfileFilter, filterData, compareFunction);
    } else if (DataType.DATETIME.equals(dataType)) {
        return "WHERE " + tsCProfile.getColName().toLowerCase()
            + " BETWEEN toDateTime(?) AND toDateTime(?) " + getFilterAndString(cProfileFilter, filterData, compareFunction);
    } else {
      throw new RuntimeException("Not supported datatype for time-series column: " + tsCProfile.getColName());
    }
  }

  @Override
  public String getOrderByClass(CProfile tsCProfile) {
    return " ORDER BY " + tsCProfile.getColName().toLowerCase();
  }

  @Override
  public String getOrderByClass(CProfile cProfile, OrderBy orderBy) {
    return " ORDER BY " + cProfile.getColName().toLowerCase() + " " + orderBy.name().toLowerCase();
  }

  @Override
  public String getLimitClass(Integer fetchSize) {
    if (fetchSize != null) {
      return " limit " + fetchSize + " ";
    }

    return "";
  }

  @Override
  public void setDateTime(CProfile tsCProfile,
                          PreparedStatement ps,
                          int parameterIndex,
                          long unixTimestamp) throws SQLException {
    DataType dataType = tsCProfile.getCsType().getDType();

    if (DataType.DATE.equals(dataType)) {
      ps.setDate(parameterIndex, new java.sql.Date(unixTimestamp));
    } else if (DataType.DATETIME.equals(dataType)) {
      ps.setLong(parameterIndex, unixTimestamp / 1000);
    } else if (DataType.TIMESTAMP.equals(dataType)
        || DataType.TIMESTAMPTZ.equals(dataType)) {
      ps.setTimestamp(parameterIndex, new Timestamp(unixTimestamp));
    } else {
      throw new RuntimeException("Not supported datatype for time-series column: " + tsCProfile.getColName());
    }
  }

  @Override
  public String getWhereClassWithCompositeFilter(CProfile tsCProfile, CompositeFilter compositeFilter) {
    StringBuilder whereClause = new StringBuilder();
    whereClause.append("WHERE ")
        .append(tsCProfile.getColName().toLowerCase())
        .append(" BETWEEN ? AND ?");

    if (compositeFilter != null && !compositeFilter.getConditions().isEmpty()) {
      whereClause.append(" AND (");

      List<String> conditions = new ArrayList<>();
      for (FilterCondition condition : compositeFilter.getConditions()) {
        String conditionStr = buildCondition(condition);
        conditions.add(conditionStr);
      }

      String joinOperator = compositeFilter.getOperator() == LogicalOperator.AND ? " AND " : " OR ";
      whereClause.append(String.join(joinOperator, conditions))
          .append(")");
    }

    return whereClause.toString();
  }

  private String buildCondition(FilterCondition condition) {
    CProfile filterProfile = condition.getCProfile();
    CompareFunction compareFunction = condition.getCompareFunction();
    String[] filterData = condition.getFilterData();

    if (filterData == null || filterData.length == 0) {
      return "";
    }

    String columnName = filterProfile.getColName().toLowerCase();
    StringBuilder conditionBuilder = new StringBuilder();
    boolean isNumeric = isNumericType(filterProfile);

    for (String value : filterData) {
      if (conditionBuilder.length() > 0) {
        conditionBuilder.append(" OR ");
      }
      if (value == null || value.trim().isEmpty()) {
        if (isNumeric) {
          conditionBuilder.append(columnName).append(" IS NULL");
        } else {
          conditionBuilder.append("(").append(columnName).append(" IS NULL OR ")
              .append(columnName).append(" = '')");
        }
      } else {
        String escapedValue = value.trim().replace("'", "''").replace("\\", "\\\\");
        if (compareFunction == CompareFunction.CONTAIN) {
          conditionBuilder.append("LOWER(").append(columnName).append(") LIKE '%")
              .append(escapedValue.toLowerCase()).append("%'");
        } else {
          conditionBuilder.append(columnName).append(" = '").append(escapedValue).append("'");
        }
      }
    }

    return conditionBuilder.length() == 0 ? "" : "(" + conditionBuilder + ")";
  }

  private String getFilterAndString(CProfile cProfileFilter, String[] filterData, CompareFunction compareFunction) {
    if (cProfileFilter == null || filterData == null) {
      return "";
    }

    String columnName = cProfileFilter.getColName().toLowerCase();
    String operator;
    boolean isNumeric = isNumericType(cProfileFilter);

    switch (compareFunction) {
      case EQUAL -> operator = "=";
      case CONTAIN -> operator = "LIKE";
      default -> {
        return "";
      }
    }

    StringBuilder filterClause = new StringBuilder();
    for (String filterValue : filterData) {
      String condition = "";

      if (filterValue == null || filterValue.isBlank()) {
        String formattedValue = filterValue;
        if (cProfileFilter.getColDbTypeName().startsWith("ENUM")) {
          Map<String, Integer> enumMap = enumParser(cProfileFilter.getColDbTypeName());
          Integer enumValue = enumMap.get(formattedValue.toLowerCase());
          if (enumValue == null) {
            continue;
          }
          formattedValue = String.valueOf(enumValue);
          operator = "=";
          condition = columnName + " " + operator + " '" + formattedValue.replace("\\", "\\\\").replace("'", "\\'") + "'";
        } else {
          if (isNumeric) {
            condition = columnName + " IS NULL";
          } else if (filterValue == null || filterValue.isEmpty()) {
            condition = columnName + " IS NULL OR " + columnName + " = ''";
          } else if (filterValue.isBlank()) {
            condition = columnName + " = '" + formattedValue + "'";
          }
        }
      } else {
        String formattedValue = filterValue;
        if (cProfileFilter.getColDbTypeName().startsWith("ENUM")) {
          Map<String, Integer> enumMap = enumParser(cProfileFilter.getColDbTypeName());
          Integer enumValue = enumMap.get(formattedValue.toLowerCase());
          if (enumValue == null) {
            continue;
          }
          formattedValue = String.valueOf(enumValue);
          operator = "=";
        } else if (compareFunction == CompareFunction.CONTAIN) {
          formattedValue = "%" + formattedValue + "%";
        }
        condition = columnName + " " + operator + " '" + formattedValue.replace("\\", "\\\\").replace("'", "\\'") + "'";
      }
      if (filterClause.length() > 0) {
        filterClause.append(" OR ");
      }
      filterClause.append(condition);
    }

    return filterClause.length() == 0 ? "" : " AND (" + filterClause + ")";
  }
}
