package ru.dimension.db.storage.dialect;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import ru.dimension.db.model.CompareFunction;
import ru.dimension.db.model.GroupFunction;
import ru.dimension.db.model.OrderBy;
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
}
