package ru.dimension.db.storage.dialect;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import ru.dimension.db.model.CompareFunction;
import ru.dimension.db.model.GroupFunction;
import ru.dimension.db.model.OrderBy;
import ru.dimension.db.model.PercentileFunction;
import ru.dimension.db.model.filter.CompositeFilter;
import ru.dimension.db.model.profile.CProfile;

public interface DatabaseDialect {

  String getSelectClassGantt(CProfile firstCProfile, CProfile secondCProfile);

  String getSelectClassStacked(GroupFunction groupFunction, CProfile cProfile);

  /**
   * Whether the underlying database has a native percentile function suitable for
   * the SUM/AVG numeric-percentile path. When false, the SQL composer falls back to
   * fetching column values and computing the percentile in-memory.
   */
  default boolean supportsNativeNumericPercentile() {
    return false;
  }

  /**
   * SELECT clause producing a single percentile aggregate (no GROUP BY).
   * Implementations MUST emit a single numeric expression that can be read via
   * {@code rs.getDouble(1)}. Only called when {@link #supportsNativeNumericPercentile()} returns true.
   */
  default String getSelectClassStackedPercentile(PercentileFunction percentileFunction,
                                                 CProfile cProfile) {
    throw new UnsupportedOperationException(
        "Native numeric percentile not supported by this dialect");
  }

  String getWhereClass(CProfile tsCProfile,
                       CProfile cProfileFilter,
                       String[] filterData,
                       CompareFunction compareFunction);

  /**
   * Used for batch pagination — excludes the left boundary
   * to avoid duplication of the last record from the previous batch.
   * The default implementation works for most dialects (Generic, PgSQL, Oracle, MSSQL, Firebird, SQLite).
   * ClickHouse overrides it because of toDateTime(?).
   */
  default String getWhereClassExcludeBegin(CProfile tsCProfile) {
    return "WHERE " + tsCProfile.getColName() + " > ? AND "
        + tsCProfile.getColName() + " <= ?";
  }

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