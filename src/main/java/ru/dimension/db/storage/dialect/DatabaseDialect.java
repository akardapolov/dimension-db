package ru.dimension.db.storage.dialect;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import ru.dimension.db.model.CompareFunction;
import ru.dimension.db.model.GroupFunction;
import ru.dimension.db.model.OrderBy;
import ru.dimension.db.model.filter.CompositeFilter;
import ru.dimension.db.model.profile.CProfile;

public interface DatabaseDialect {

  // Select class
  String getSelectClassGantt(CProfile firstCProfile, CProfile secondCProfile);

  String getSelectClassStacked(GroupFunction groupFunction, CProfile cProfile);

  // Where class
  String getWhereClass(CProfile tsCProfile,
                       CProfile cProfileFilter,
                       String[] filterData,
                       CompareFunction compareFunction);

  // Order by class
  String getOrderByClass(CProfile tsCProfile);
  String getOrderByClass(CProfile cProfile, OrderBy orderBy);

  // Limit class
  String getLimitClass(Integer fetchSize);

  // Set date time for parameter
  void setDateTime(CProfile tsCProfile,
                   PreparedStatement ps,
                   int parameterIndex,
                   long dateTime) throws SQLException;

  String getWhereClassWithCompositeFilter(CProfile tsCProfile,
                                          CompositeFilter compositeFilter);

  default boolean isNumericType(CProfile cProfile) {
    String typeName = cProfile.getColDbTypeName().toUpperCase();
    return typeName.contains("INT") ||
        typeName.contains("NUMERIC") ||
        typeName.contains("DECIMAL") ||
        typeName.contains("FLOAT") ||
        typeName.contains("DOUBLE") ||
        typeName.contains("REAL");
  }
}
