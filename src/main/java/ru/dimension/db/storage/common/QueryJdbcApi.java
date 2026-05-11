package ru.dimension.db.storage.common;

import static ru.dimension.db.service.mapping.Mapper.convertRawToLong;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.dbcp2.BasicDataSource;
import ru.dimension.db.model.GranularityFunction;
import ru.dimension.db.model.GroupFunction;
import ru.dimension.db.model.OrderBy;
import ru.dimension.db.model.PercentileFunction;
import ru.dimension.db.model.filter.CompositeFilter;
import ru.dimension.db.model.output.GanttColumnCount;
import ru.dimension.db.model.output.GanttColumnSum;
import ru.dimension.db.model.output.StackedColumn;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.table.TType;
import ru.dimension.db.sql.BatchResultSet;
import ru.dimension.db.sql.BatchResultSetSqlImpl;
import ru.dimension.db.storage.dialect.DatabaseDialect;
import ru.dimension.db.util.PercentileUtil;

@Log4j2
public abstract class QueryJdbcApi {

  protected final BasicDataSource basicDataSource;

  protected QueryJdbcApi(BasicDataSource basicDataSource) {
    this.basicDataSource = basicDataSource;
  }

  protected List<StackedColumn> getStackedCommon(String tableName,
                                                 CProfile tsCProfile,
                                                 CProfile cProfile,
                                                 GroupFunction groupFunction,
                                                 CompositeFilter compositeFilter,
                                                 long begin,
                                                 long end,
                                                 DatabaseDialect databaseDialect) {
    List<StackedColumn> results = new ArrayList<>();
    String colName = cProfile.getColName().toLowerCase();
    String whereClass = databaseDialect.getWhereClassWithCompositeFilter(tsCProfile, compositeFilter);

    try (Connection conn = basicDataSource.getConnection()) {

      if (GroupFunction.COUNT.equals(groupFunction)) {
        String selectClass = databaseDialect.getSelectClassStacked(groupFunction, cProfile);
        String query = getQueryStackedCommon(tableName, colName, groupFunction, selectClass, whereClass);

        log.info("Query: {}", query);

        try (PreparedStatement ps = conn.prepareStatement(query)) {
          int paramIndex = 1;
          databaseDialect.setDateTime(tsCProfile, ps, paramIndex++, begin);
          databaseDialect.setDateTime(tsCProfile, ps, paramIndex++, end);

          try (ResultSet rs = ps.executeQuery()) {
            StackedColumn column = new StackedColumn();
            column.setKey(begin);
            column.setTail(end);

            while (rs.next()) {
              fillKeyData(rs, groupFunction, column);
            }

            results.add(column);
          }
        }

        return results;
      }

      String aggregateExpr = GroupFunction.AVG.equals(groupFunction)
          ? "AVG(" + colName + ")"
          : "SUM(" + colName + ")";

      String query = "SELECT " + aggregateExpr + " AS value FROM " + tableName + " " + whereClass;

      log.info("Query: {}", query);

      try (PreparedStatement ps = conn.prepareStatement(query)) {
        int paramIndex = 1;
        databaseDialect.setDateTime(tsCProfile, ps, paramIndex++, begin);
        databaseDialect.setDateTime(tsCProfile, ps, paramIndex++, end);

        try (ResultSet rs = ps.executeQuery()) {
          StackedColumn column = new StackedColumn();
          column.setKey(begin);
          column.setTail(end);

          if (rs.next()) {
            double value = rs.getDouble(1);
            if (rs.wasNull()) {
              value = 0D;
            }

            if (GroupFunction.AVG.equals(groupFunction)) {
              column.getKeyAvg().put(cProfile.getColName(), value);
            } else {
              column.getKeySum().put(cProfile.getColName(), value);
            }
          }

          results.add(column);
        }
      }

      return results;
    } catch (SQLException e) {
      log.info("Query: {}", tableName);
      throw new RuntimeException("Error executing stacked query with composite filter: " + e.getMessage(), e);
    }
  }

  protected List<StackedColumn> getStackedCommonRegular(String tableName,
                                                        CProfile cProfile,
                                                        GroupFunction groupFunction,
                                                        CompositeFilter compositeFilter,
                                                        int limit,
                                                        DatabaseDialect databaseDialect) {
    List<StackedColumn> results = new ArrayList<>();

    String colName = cProfile.getColName().toLowerCase();
    String selectClass = databaseDialect.getSelectClassStacked(groupFunction, cProfile);

    String whereClause = "";
    if (compositeFilter != null && compositeFilter.getConditions() != null && !compositeFilter.getConditions().isEmpty()) {
      whereClause = databaseDialect.getWhereClassWithCompositeFilterNoTimestamp(compositeFilter);
    }

    String limitClause = databaseDialect.getLimitClass(limit);
    String orderForOffset = limitClause.toUpperCase().contains("OFFSET") ? " ORDER BY (SELECT NULL) " : "";

    String subQuery = "(SELECT * FROM " + tableName + " "
        + whereClause
        + orderForOffset
        + limitClause + ") sub_t";

    String query;
    if (GroupFunction.COUNT.equals(groupFunction) ||
        GroupFunction.SUM.equals(groupFunction) ||
        GroupFunction.AVG.equals(groupFunction)) {
      query = selectClass + "FROM " + subQuery + " GROUP BY " + colName;
    } else {
      throw new RuntimeException("Not supported group function: " + groupFunction);
    }

    try (Connection conn = basicDataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(query)) {

      ResultSet rs = ps.executeQuery();

      StackedColumn column = new StackedColumn();
      column.setKey(0);
      column.setTail(0);

      while (rs.next()) {
        fillKeyData(rs, groupFunction, column);
      }

      results.add(column);
    } catch (SQLException e) {
      log.info("Query: " + query);
      throw new RuntimeException("Error executing stacked query for regular table: " + e.getMessage(), e);
    }

    return results;
  }

  private String getQueryStackedCommon(String tableName,
                                       String colName,
                                       GroupFunction groupFunction,
                                       String selectClass,
                                       String whereClass) {
    if (GroupFunction.COUNT.equals(groupFunction) ||
        GroupFunction.SUM.equals(groupFunction) ||
        GroupFunction.AVG.equals(groupFunction)) {
      return selectClass +
          "FROM " + tableName + " " +
          whereClass +
          " GROUP BY " + colName;
    } else {
      throw new RuntimeException("Not supported group function: " + groupFunction);
    }
  }

  protected List<StackedColumn> getStackedPercentileNumericCommon(String tableName,
                                                                  CProfile tsCProfile,
                                                                  CProfile cProfile,
                                                                  PercentileFunction percentileFunction,
                                                                  CompositeFilter compositeFilter,
                                                                  long begin,
                                                                  long end,
                                                                  DatabaseDialect databaseDialect) {
    String whereClass = databaseDialect.getWhereClassWithCompositeFilter(tsCProfile, compositeFilter);
    String colNameOriginal = cProfile.getColName();
    double percentileValue;

    if (databaseDialect.supportsNativeNumericPercentile()) {
      String selectClass = databaseDialect.getSelectClassStackedPercentile(percentileFunction, cProfile);
      String query = selectClass + "FROM " + tableName + " " + whereClass;
      log.info("Query: {}", query);

      try (Connection conn = basicDataSource.getConnection();
          PreparedStatement ps = conn.prepareStatement(query)) {
        int paramIndex = 1;
        databaseDialect.setDateTime(tsCProfile, ps, paramIndex++, begin);
        databaseDialect.setDateTime(tsCProfile, ps, paramIndex++, end);

        try (ResultSet rs = ps.executeQuery()) {
          if (!rs.next()) {
            return List.of();
          }
          percentileValue = rs.getDouble(1);
          if (rs.wasNull()) {
            return List.of();
          }
        }
      } catch (SQLException e) {
        throw new RuntimeException("Error executing native percentile query: " + e.getMessage(), e);
      }
    } else {
      List<Double> values = new ArrayList<>();
      String colName = cProfile.getColName().toLowerCase();
      String query = "SELECT " + colName + " FROM " + tableName + " " + whereClass;
      log.info("Query: {}", query);

      try (Connection conn = basicDataSource.getConnection();
          PreparedStatement ps = conn.prepareStatement(query)) {
        int paramIndex = 1;
        databaseDialect.setDateTime(tsCProfile, ps, paramIndex++, begin);
        databaseDialect.setDateTime(tsCProfile, ps, paramIndex++, end);

        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            double v = rs.getDouble(1);
            if (!rs.wasNull()) {
              values.add(v);
            }
          }
        }
      } catch (SQLException e) {
        throw new RuntimeException("Error executing in-memory percentile fetch: " + e.getMessage(), e);
      }

      if (values.isEmpty()) {
        return List.of();
      }
      percentileValue = PercentileUtil.compute(values, percentileFunction.getValue());
    }

    Map<String, Double> keyPercentile = new HashMap<>();
    keyPercentile.put(colNameOriginal, percentileValue);

    StackedColumn column = StackedColumn.builder()
        .key(begin)
        .tail(end)
        .keyPercentile(keyPercentile)
        .build();
    return List.of(column);
  }

  protected List<StackedColumn> getStackedPercentileCountCommon(String tableName,
                                                                CProfile tsCProfile,
                                                                CProfile cProfile,
                                                                PercentileFunction percentileFunction,
                                                                GranularityFunction granularityFunction,
                                                                CompositeFilter compositeFilter,
                                                                long begin,
                                                                long end,
                                                                DatabaseDialect databaseDialect) {
    GranularityFunction resolved = GranularityFunction.resolve(granularityFunction, begin, end);
    long bucketSize = resolved.getMillis();
    List<long[]> buckets = PercentileUtil.buildBuckets(begin, end, bucketSize);

    Map<String, List<Double>> groupValues = new LinkedHashMap<>();
    int nonEmptyBuckets = 0;

    for (int b = 0; b < buckets.size(); b++) {
      long[] bucket = buckets.get(b);
      boolean isLast = (b == buckets.size() - 1);
      long effectiveBucketEnd = isLast ? bucket[1] : bucket[1] - 1;

      List<StackedColumn> bucketResult = getStackedCommon(tableName, tsCProfile, cProfile,
                                                          GroupFunction.COUNT, compositeFilter,
                                                          bucket[0], effectiveBucketEnd,
                                                          databaseDialect);

      Map<String, Integer> bucketCounts = bucketResult.isEmpty()
          ? Map.of()
          : bucketResult.get(0).getKeyCount();

      if (bucketCounts == null || bucketCounts.isEmpty()) {
        continue;
      }

      nonEmptyBuckets++;
      for (Map.Entry<String, Integer> entry : bucketCounts.entrySet()) {
        groupValues
            .computeIfAbsent(entry.getKey(), k -> new ArrayList<>())
            .add(entry.getValue().doubleValue());
      }
    }

    if (nonEmptyBuckets < PercentileUtil.MIN_BUCKETS_FOR_PERCENTILE) {
      log.warn("Not enough data buckets ({}) for percentile calculation on column '{}', falling back to regular COUNT",
               nonEmptyBuckets, cProfile.getColName());
      return getStackedCommon(tableName, tsCProfile, cProfile,
                              GroupFunction.COUNT, compositeFilter, begin, end, databaseDialect);
    }

    Map<String, Double> keyPercentile = new LinkedHashMap<>();
    for (Map.Entry<String, List<Double>> entry : groupValues.entrySet()) {
      double p = PercentileUtil.compute(entry.getValue(), percentileFunction.getValue());
      keyPercentile.put(entry.getKey(), p);
    }

    StackedColumn column = StackedColumn.builder()
        .key(begin)
        .tail(end)
        .keyPercentile(keyPercentile)
        .build();
    return List.of(column);
  }

  protected List<GanttColumnCount> getGanttCountCommon(String tableName,
                                                       CProfile tsCProfile,
                                                       CProfile firstGrpBy,
                                                       CProfile secondGrpBy,
                                                       CompositeFilter compositeFilter,
                                                       long begin,
                                                       long end,
                                                       DatabaseDialect databaseDialect) {
    List<GanttColumnCount> ganttColumnCounts = new ArrayList<>();

    String firstColName = firstGrpBy.getColName().toLowerCase();
    String secondColName = secondGrpBy.getColName().toLowerCase();

    String query =
        databaseDialect.getSelectClassGantt(firstGrpBy, secondGrpBy) +
            " FROM " + tableName + " " +
            databaseDialect.getWhereClassWithCompositeFilter(tsCProfile, compositeFilter) +
            " GROUP BY " + firstColName + ", " + secondColName;
    log.info("Query: " + query);

    Map<String, Map<String, Integer>> map = new LinkedHashMap<>();

    try (Connection conn = basicDataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(query)) {

      int paramIndex = 1;
      databaseDialect.setDateTime(tsCProfile, ps, paramIndex++, begin);
      databaseDialect.setDateTime(tsCProfile, ps, paramIndex++, end);

      ResultSet rs = ps.executeQuery();

      while (rs.next()) {
        String key = rs.getString(1);
        String keyGantt = rs.getString(2);
        int countGantt = rs.getInt(3);

        if (Objects.isNull(key)) {
          key = "";
        }
        if (Objects.isNull(keyGantt)) {
          keyGantt = "";
        }

        map.computeIfAbsent(key, k -> new HashMap<>()).put(keyGantt, countGantt);
      }
    } catch (SQLException e) {
      throw new RuntimeException("Error executing Gantt query with composite filter: " + e.getMessage(), e);
    }

    for (Map.Entry<String, Map<String, Integer>> entry : map.entrySet()) {
      GanttColumnCount column = new GanttColumnCount(entry.getKey(), new LinkedHashMap<>(entry.getValue()));
      ganttColumnCounts.add(column);
    }

    return ganttColumnCounts;
  }

  protected List<GanttColumnCount> getGanttCountCommonRegular(String tableName,
                                                              CProfile firstGrpBy,
                                                              CProfile secondGrpBy,
                                                              CompositeFilter compositeFilter,
                                                              int limit,
                                                              DatabaseDialect databaseDialect) {
    List<GanttColumnCount> ganttColumnCounts = new ArrayList<>();

    String firstColName = firstGrpBy.getColName().toLowerCase();
    String secondColName = secondGrpBy.getColName().toLowerCase();

    String whereClause = "";
    if (compositeFilter != null && compositeFilter.getConditions() != null && !compositeFilter.getConditions().isEmpty()) {
      whereClause = databaseDialect.getWhereClassWithCompositeFilterNoTimestamp(compositeFilter);
    }

    String limitClause = databaseDialect.getLimitClass(limit);
    String orderForOffset = limitClause.toUpperCase().contains("OFFSET") ? " ORDER BY (SELECT NULL) " : "";

    String subQuery = "(SELECT * FROM " + tableName + " "
        + whereClause
        + orderForOffset
        + limitClause + ") sub_t";

    String query =
        "SELECT " + firstColName + ", " + secondColName + ", COUNT(*) " +
            " FROM " + subQuery +
            " GROUP BY " + firstColName + ", " + secondColName;
    log.info("Query: " + query);

    Map<String, Map<String, Integer>> map = new LinkedHashMap<>();

    try (Connection conn = basicDataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(query)) {

      ResultSet rs = ps.executeQuery();

      while (rs.next()) {
        String key = rs.getString(1);
        String keyGantt = rs.getString(2);
        int countGantt = rs.getInt(3);

        if (Objects.isNull(key)) {
          key = "";
        }
        if (Objects.isNull(keyGantt)) {
          keyGantt = "";
        }

        map.computeIfAbsent(key, k -> new HashMap<>()).put(keyGantt, countGantt);
      }
    } catch (SQLException e) {
      throw new RuntimeException("Error executing Gantt query for regular table: " + e.getMessage(), e);
    }

    for (Map.Entry<String, Map<String, Integer>> entry : map.entrySet()) {
      GanttColumnCount column = new GanttColumnCount(entry.getKey(), new LinkedHashMap<>(entry.getValue()));
      ganttColumnCounts.add(column);
    }

    return ganttColumnCounts;
  }

  private void fillKeyData(ResultSet rs,
                           GroupFunction groupFunction,
                           StackedColumn column) throws SQLException {
    String key = rs.getString(1);

    if (Objects.isNull(key)) {
      key = "";
    }

    if (GroupFunction.COUNT.equals(groupFunction)) {
      int count = rs.getInt(2);
      column.getKeyCount().put(key, count);
    } else if (GroupFunction.SUM.equals(groupFunction)) {
      double sum = rs.getDouble(2);
      column.getKeySum().put(key, sum);
    } else if (GroupFunction.AVG.equals(groupFunction)) {
      double avg = rs.getDouble(2);
      column.getKeyAvg().put(key, avg);
    } else {
      throw new RuntimeException("Not supported");
    }
  }

  protected List<GanttColumnSum> getGanttSumCommon(String tableName,
                                                   CProfile tsCProfile,
                                                   CProfile firstGrpBy,
                                                   CProfile secondGrpBy,
                                                   CompositeFilter compositeFilter,
                                                   long begin,
                                                   long end,
                                                   DatabaseDialect databaseDialect) {
    List<GanttColumnSum> results = new ArrayList<>();
    String firstColName = firstGrpBy.getColName().toLowerCase();
    String secondColName = secondGrpBy.getColName().toLowerCase();

    String query =
        "SELECT " + firstColName + ", SUM(" + secondColName + ") " +
            "FROM " + tableName + " " +
            databaseDialect.getWhereClassWithCompositeFilter(tsCProfile, compositeFilter) +
            " GROUP BY " + firstColName;
    log.info("Query: " + query);

    try (Connection conn = basicDataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(query)) {

      int paramIndex = 1;
      databaseDialect.setDateTime(tsCProfile, ps, paramIndex++, begin);
      databaseDialect.setDateTime(tsCProfile, ps, paramIndex++, end);

      ResultSet rs = ps.executeQuery();

      while (rs.next()) {
        String key = rs.getString(1);
        double sum = rs.getDouble(2);
        if (key == null) key = "";
        results.add(new GanttColumnSum(key, sum));
      }
    } catch (SQLException e) {
      throw new RuntimeException("Error executing gantt sum with composite filter query: " + e.getMessage(), e);
    }

    return results;
  }

  protected List<GanttColumnSum> getGanttSumCommonRegular(String tableName,
                                                          CProfile firstGrpBy,
                                                          CProfile secondGrpBy,
                                                          CompositeFilter compositeFilter,
                                                          int limit,
                                                          DatabaseDialect databaseDialect) {
    List<GanttColumnSum> results = new ArrayList<>();
    String firstColName = firstGrpBy.getColName().toLowerCase();
    String secondColName = secondGrpBy.getColName().toLowerCase();

    String whereClause = "";
    if (compositeFilter != null && compositeFilter.getConditions() != null && !compositeFilter.getConditions().isEmpty()) {
      whereClause = databaseDialect.getWhereClassWithCompositeFilterNoTimestamp(compositeFilter);
    }

    String limitClause = databaseDialect.getLimitClass(limit);
    String orderForOffset = limitClause.toUpperCase().contains("OFFSET") ? " ORDER BY (SELECT NULL) " : "";

    String subQuery = "(SELECT * FROM " + tableName + " "
        + whereClause
        + orderForOffset
        + limitClause + ") sub_t";

    String query =
        "SELECT " + firstColName + ", SUM(" + secondColName + ") " +
            "FROM " + subQuery +
            " GROUP BY " + firstColName;
    log.info("Query: " + query);

    try (Connection conn = basicDataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(query)) {

      ResultSet rs = ps.executeQuery();

      while (rs.next()) {
        String key = rs.getString(1);
        double sum = rs.getDouble(2);
        if (key == null) key = "";
        results.add(new GanttColumnSum(key, sum));
      }
    } catch (SQLException e) {
      throw new RuntimeException("Error executing gantt sum for regular table: " + e.getMessage(), e);
    }

    return results;
  }

  protected List<String> getDistinctCommon(String tableName,
                                           CProfile tsCProfile,
                                           CProfile cProfile,
                                           OrderBy orderBy,
                                           CompositeFilter compositeFilter,
                                           int limit,
                                           long begin,
                                           long end,
                                           DatabaseDialect databaseDialect) {
    String colName = cProfile.getColName().toLowerCase();

    String query =
        "SELECT DISTINCT " + colName +
            " FROM " + tableName + " " +
            databaseDialect.getWhereClassWithCompositeFilter(tsCProfile, compositeFilter) +
            databaseDialect.getOrderByClass(cProfile, orderBy) +
            databaseDialect.getLimitClass(limit);
    log.info("Query: " + query);

    List<String> distinctValues = new ArrayList<>();

    try (Connection conn = basicDataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(query)) {

      int paramIndex = 1;
      databaseDialect.setDateTime(tsCProfile, ps, paramIndex++, begin);
      databaseDialect.setDateTime(tsCProfile, ps, paramIndex++, end);

      ResultSet rs = ps.executeQuery();

      while (rs.next()) {
        String val = rs.getString(1);
        distinctValues.add(Objects.requireNonNullElse(val, ""));
      }
    } catch (SQLException e) {
      throw new RuntimeException("Error getting distinct values with composite filter: " + e.getMessage(), e);
    }

    return distinctValues;
  }

  protected List<String> getDistinctCommonRegular(String tableName,
                                                  CProfile cProfile,
                                                  OrderBy orderBy,
                                                  CompositeFilter compositeFilter,
                                                  int limit,
                                                  DatabaseDialect databaseDialect) {
    String colName = cProfile.getColName().toLowerCase();

    String whereClause = "";
    if (compositeFilter != null && compositeFilter.getConditions() != null && !compositeFilter.getConditions().isEmpty()) {
      whereClause = databaseDialect.getWhereClassWithCompositeFilterNoTimestamp(compositeFilter);
    }

    String query =
        "SELECT DISTINCT " + colName +
            " FROM " + tableName + " " +
            whereClause +
            databaseDialect.getOrderByClass(cProfile, orderBy) +
            databaseDialect.getLimitClass(limit);
    log.info("Query: " + query);

    List<String> distinctValues = new ArrayList<>();

    try (Connection conn = basicDataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(query)) {

      ResultSet rs = ps.executeQuery();

      while (rs.next()) {
        String val = rs.getString(1);
        distinctValues.add(Objects.requireNonNullElse(val, ""));
      }
    } catch (SQLException e) {
      throw new RuntimeException("Error getting distinct values for regular table: " + e.getMessage(), e);
    }

    return distinctValues;
  }

  public List<String> getDistinctCommon(String tableName,
                                        CProfile tsCProfile,
                                        CProfile cProfile,
                                        OrderBy orderBy,
                                        int limit,
                                        long begin,
                                        long end,
                                        DatabaseDialect databaseDialect) {

    String colName = cProfile.getColName().toLowerCase();

    String query =
        "SELECT DISTINCT " + colName +
            " FROM " + tableName + " " +
            databaseDialect.getWhereClass(tsCProfile, null, null, null) +
            databaseDialect.getOrderByClass(cProfile, orderBy) +
            databaseDialect.getLimitClass(limit);
    log.info("Query: " + query);

    List<String> distinctValues = new ArrayList<>();

    try (Connection conn = basicDataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(query)) {

      databaseDialect.setDateTime(tsCProfile, ps, 1, begin);
      databaseDialect.setDateTime(tsCProfile, ps, 2, end);

      ResultSet rs = ps.executeQuery();

      while (rs.next()) {
        String val = rs.getString(1);
        distinctValues.add(Objects.requireNonNullElse(val, ""));
      }
    } catch (SQLException e) {
      throw new RuntimeException("Error getting distinct values: " + e.getMessage(), e);
    }

    return distinctValues;
  }

  protected BatchResultSet getBatchResultSetCommon(String tableName,
                                                   long begin,
                                                   long end,
                                                   int fetchSize,
                                                   List<CProfile> cProfiles,
                                                   DatabaseDialect databaseDialect) {
    Optional<CProfile> tsCProfile = cProfiles.stream()
        .filter(k -> k.getCsType().isTimeStamp())
        .findAny();

    if (tsCProfile.isPresent()) {
      long maxBlockId = getLastBlockIdLocal(tableName, tsCProfile.get(), begin, end, databaseDialect);
      return new BatchResultSetSqlImpl(tableName, fetchSize, begin, end, maxBlockId,
                                       cProfiles, basicDataSource, databaseDialect, TType.TIME_SERIES);
    }

    long totalRows = getRowCount(tableName);
    return new BatchResultSetSqlImpl(tableName, fetchSize, 0, totalRows, totalRows,
                                     cProfiles, basicDataSource, databaseDialect, TType.REGULAR);
  }

  protected long getRowCount(String tableName) {
    long count = 0;
    String query = "SELECT COUNT(*) FROM " + tableName;

    try (Connection conn = basicDataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(query)) {

      ResultSet rs = ps.executeQuery();
      if (rs.next()) {
        count = rs.getLong(1);
      }
    } catch (SQLException e) {
      throw new RuntimeException("Error getting row count: " + e.getMessage(), e);
    }

    return count;
  }

  protected long getFirstBlockIdLocal(String tableName,
                                      CProfile tsCProfile,
                                      long begin,
                                      long end,
                                      DatabaseDialect databaseDialect) {
    long lastBlockId = 0L;

    String query = "";

    String colName = tsCProfile.getColName().toLowerCase();
    if (Long.MIN_VALUE == begin) {
      query =
          "SELECT MIN(" + colName + ") " +
              "FROM " + tableName;
    } else {
      query =
          "SELECT MIN(" + colName + ") " +
              "FROM " + tableName + " " +
              databaseDialect.getWhereClass(tsCProfile, null, null, null);
    }

    try (Connection conn = basicDataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(query)) {

      if (Long.MIN_VALUE == begin) {

      } else {
        databaseDialect.setDateTime(tsCProfile, ps, 1, begin);
        databaseDialect.setDateTime(tsCProfile, ps, 2, end);
      }

      ResultSet rs = ps.executeQuery();

      while (rs.next()) {
        Object object = rs.getObject(1);
        lastBlockId = convertRawToLong(object, tsCProfile);
      }

    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    return lastBlockId;
  }

  protected long getLastBlockIdLocal(String tableName,
                                     CProfile tsCProfile,
                                     long begin,
                                     long end,
                                     DatabaseDialect databaseDialect) {
    long lastBlockId = 0L;

    String query = "";

    String colName = tsCProfile.getColName().toLowerCase();
    if (Long.MAX_VALUE == end) {
      query =
          "SELECT MAX(" + colName + ") " +
              "FROM " + tableName;
    } else {
      query =
          "SELECT MAX(" + colName + ") " +
              "FROM " + tableName + " " +
              databaseDialect.getWhereClass(tsCProfile, null, null, null);
    }

    try (Connection conn = basicDataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(query)) {

      if (Long.MAX_VALUE == end) {

      } else {
        databaseDialect.setDateTime(tsCProfile, ps, 1, begin);
        databaseDialect.setDateTime(tsCProfile, ps, 2, end);
      }

      ResultSet rs = ps.executeQuery();

      while (rs.next()) {
        Object object = rs.getObject(1);
        lastBlockId = convertRawToLong(object, tsCProfile);
      }

    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    return lastBlockId;
  }

  protected static void checkDataType(CProfile cProfile, String dataType) {
    boolean containsIgnoreCase = cProfile.getColDbTypeName()
        .regionMatches(true,
                       cProfile.getColDbTypeName().indexOf(dataType),
                       dataType,
                       0,
                       dataType.length());

    if (containsIgnoreCase) {
      throw new RuntimeException("Not supported for " + dataType);
    }
  }
}