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
import lombok.extern.log4j.Log4j2;
import org.apache.commons.dbcp2.BasicDataSource;
import ru.dimension.db.model.GroupFunction;
import ru.dimension.db.model.OrderBy;
import ru.dimension.db.model.filter.CompositeFilter;
import ru.dimension.db.model.output.GanttColumnCount;
import ru.dimension.db.model.output.GanttColumnSum;
import ru.dimension.db.model.output.StackedColumn;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.sql.BatchResultSet;
import ru.dimension.db.sql.BatchResultSetSqlImpl;
import ru.dimension.db.storage.dialect.DatabaseDialect;

@Log4j2
public abstract class QueryJdbcApi {

  private final BasicDataSource basicDataSource;

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
    String selectClass = databaseDialect.getSelectClassStacked(groupFunction, cProfile);
    String whereClass = databaseDialect.getWhereClassWithCompositeFilter(tsCProfile, compositeFilter);

    String query = getQueryStackedCommon(tableName, colName, groupFunction, selectClass, whereClass);

    try (Connection conn = basicDataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(query)) {

      int paramIndex = 1;
      databaseDialect.setDateTime(tsCProfile, ps, paramIndex++, begin);
      databaseDialect.setDateTime(tsCProfile, ps, paramIndex++, end);

      ResultSet rs = ps.executeQuery();

      StackedColumn column = new StackedColumn();
      column.setKey(begin);
      column.setTail(end);

      while (rs.next()) {
        fillKeyData(rs, groupFunction, column);
      }

      results.add(column);
    } catch (SQLException e) {
      log.info("Query: " + query);
      throw new RuntimeException("Error executing stacked query with composite filter: " + e.getMessage(), e);
    }

    return results;
  }

  private String getQueryStackedCommon(String tableName,
                                       String colName,
                                       GroupFunction groupFunction,
                                       String selectClass,
                                       String whereClass) {
    if (GroupFunction.COUNT.equals(groupFunction)) {
      return selectClass +
          "FROM " + tableName + " " +
          whereClass +
          " GROUP BY " + colName;
    } else if (GroupFunction.SUM.equals(groupFunction) || GroupFunction.AVG.equals(groupFunction)) {
      return selectClass +
          "FROM " + tableName + " " +
          whereClass;
    } else {
      throw new RuntimeException("Not supported group function: " + groupFunction);
    }
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

      // Set timestamp parameters
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
    CProfile tsCProfile = cProfiles.stream()
        .filter(k -> k.getCsType().isTimeStamp())
        .findAny()
        .orElseThrow(() -> new RuntimeException("API working only for time-series tables"));

    long maxBlockId = getLastBlockIdLocal(tableName, tsCProfile, begin, end, databaseDialect);

    return new BatchResultSetSqlImpl(tableName, fetchSize, begin, end, maxBlockId, cProfiles, basicDataSource, databaseDialect);
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
