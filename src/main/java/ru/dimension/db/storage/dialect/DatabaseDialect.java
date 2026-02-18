package ru.dimension.db.storage.dialect;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import ru.dimension.db.model.CompareFunction;
import ru.dimension.db.model.GroupFunction;
import ru.dimension.db.model.OrderBy;
import ru.dimension.db.model.filter.CompositeFilter;
import ru.dimension.db.model.profile.CProfile;

public interface DatabaseDialect {

  String getSelectClassGantt(CProfile firstCProfile, CProfile secondCProfile);

  String getSelectClassStacked(GroupFunction groupFunction, CProfile cProfile);

  String getWhereClass(CProfile tsCProfile,
                       CProfile cProfileFilter,
                       String[] filterData,
                       CompareFunction compareFunction);

  String getOrderByClass(CProfile tsCProfile);
  String getOrderByClass(CProfile cProfile, OrderBy orderBy);

  String getLimitClass(Integer fetchSize);

  String getOffsetClass(int offset);

  void setDateTime(CProfile tsCProfile,
                   PreparedStatement ps,
                   int parameterIndex,
                   long dateTime) throws SQLException;

  String getWhereClassWithCompositeFilter(CProfile tsCProfile,
                                          CompositeFilter compositeFilter);

  String getWhereClassWithCompositeFilterNoTimestamp(CompositeFilter compositeFilter);

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