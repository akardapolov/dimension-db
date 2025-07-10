package ru.dimension.db.storage.dialect;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import ru.dimension.db.model.CompareFunction;
import ru.dimension.db.model.GroupFunction;
import ru.dimension.db.model.OrderBy;
import ru.dimension.db.model.profile.CProfile;

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
      whereClause.append(" AND (");
      String column = cProfileFilter.getColName();

      for (int i = 0; i < filterData.length; i++) {
        if (i > 0) whereClause.append(" OR ");

        if (CompareFunction.CONTAIN.equals(compareFunction)) {
          whereClause.append(column).append(" LIKE ?");
        } else {
          whereClause.append(column).append(" = ?");
        }
      }
      whereClause.append(")");
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
  public void setDateTime(CProfile tsCProfile, PreparedStatement ps,
                          int parameterIndex, long unixTimestamp) throws SQLException {
    ps.setTimestamp(parameterIndex, new Timestamp(unixTimestamp));
  }
}