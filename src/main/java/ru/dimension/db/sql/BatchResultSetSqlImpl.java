package ru.dimension.db.sql;

import static ru.dimension.db.service.mapping.Mapper.convertRawToLong;
import static ru.dimension.db.service.mapping.Mapper.convertRawToString;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.dbcp2.BasicDataSource;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.cstype.CType;
import ru.dimension.db.model.profile.table.TType;
import ru.dimension.db.service.CommonServiceApi;
import ru.dimension.db.storage.dialect.DatabaseDialect;
import ru.dimension.db.storage.dialect.FirebirdDialect;
import ru.dimension.db.storage.dialect.MsSqlDialect;
import ru.dimension.db.storage.dialect.OracleDialect;

@Log4j2
public class BatchResultSetSqlImpl extends CommonServiceApi implements BatchResultSet {

  private final String tableName;

  private final long begin;
  private final long end;

  private final int fetchSize;
  private final List<CProfile> cProfiles;

  private Entry<Long, Integer> pointer;

  private boolean isNext = true;
  private boolean isStarted = true;

  private final long maxBlockId;

  private final BasicDataSource basicDataSource;

  private final DatabaseDialect databaseDialect;

  private final TType tableType;

  private int currentOffset;

  public BatchResultSetSqlImpl(String tableName,
                               int fetchSize,
                               long begin,
                               long end,
                               long maxBlockId,
                               List<CProfile> cProfiles,
                               BasicDataSource basicDataSource,
                               DatabaseDialect databaseDialect) {
    this(tableName, fetchSize, begin, end, maxBlockId, cProfiles, basicDataSource, databaseDialect, TType.TIME_SERIES);
  }

  public BatchResultSetSqlImpl(String tableName,
                               int fetchSize,
                               long begin,
                               long end,
                               long maxBlockId,
                               List<CProfile> cProfiles,
                               BasicDataSource basicDataSource,
                               DatabaseDialect databaseDialect,
                               TType tableType) {
    this.tableName = tableName;
    this.fetchSize = fetchSize;
    this.begin = begin;
    this.end = end;
    this.cProfiles = cProfiles;
    this.basicDataSource = basicDataSource;
    this.databaseDialect = databaseDialect;
    this.tableType = tableType;

    this.pointer = Map.entry(begin, 0);
    this.maxBlockId = maxBlockId;
    this.currentOffset = 0;
  }

  @Override
  public List<List<Object>> getObject() {
    if (tableType == TType.REGULAR) {
      return getObjectRegular();
    }
    return getObjectTimeSeries();
  }

  private List<List<Object>> getObjectRegular() {
    List<List<Object>> tableColFormatData = new ArrayList<>();

    String query = getSqlQueryRegular();

    AtomicInteger fetchCounter = new AtomicInteger(0);

    try (Connection connection = basicDataSource.getConnection();
        PreparedStatement ps = connection.prepareStatement(query)) {

      ResultSet rs = ps.executeQuery();

      for (int i = 0; i < cProfiles.size(); i++) {
        tableColFormatData.add(new ArrayList<>());
      }

      while (rs.next()) {
        for (int i = 0; i < cProfiles.size(); i++) {
          CProfile cProfile = cProfiles.get(i);
          Object cellValue = rs.getObject(cProfile.getColName());

          if (cProfile.getCsType().getCType().equals(CType.STRING)) {
            tableColFormatData.get(i).add(convertRawToString(cellValue, cProfile));
          } else {
            tableColFormatData.get(i).add(cellValue);
          }
        }
        fetchCounter.incrementAndGet();
      }
    } catch (SQLException e) {
      log.catching(e);
    }

    currentOffset += fetchCounter.get();

    if (fetchCounter.get() == 0) {
      isNext = false;
    } else if (fetchCounter.get() < fetchSize) {
      isNext = false;
    }

    if (currentOffset >= maxBlockId) {
      isNext = false;
    }

    return transpose(tableColFormatData);
  }

  private List<List<Object>> getObjectTimeSeries() {
    List<List<Object>> tableColFormatData = new ArrayList<>();

    AtomicReference<Entry<Long, Integer>> pointerLocal =
        new AtomicReference<>(Map.entry(isStarted ? 0L : pointer.getKey(), isStarted ? 0 : pointer.getValue()));

    Optional<CProfile> tsCProfile = cProfiles.stream()
        .filter(k -> k.getCsType().isTimeStamp())
        .findAny();

    if (tsCProfile.isEmpty()) {
      throw new RuntimeException("API working only for time-series tables");
    }

    String query = getSqlQueryTimeSeries(tsCProfile);

    AtomicInteger fetchCounter = new AtomicInteger(pointer.getValue());

    try (Connection connection = basicDataSource.getConnection();
        PreparedStatement ps = connection.prepareStatement(query)) {

      long dateTime = isStarted ? pointer.getKey() : pointer.getKey() + 1;
      databaseDialect.setDateTime(tsCProfile.get(), ps, 1, dateTime);
      databaseDialect.setDateTime(tsCProfile.get(), ps, 2, maxBlockId);

      ResultSet rs = ps.executeQuery();

      for (int i = 0; i < cProfiles.size(); i++) {
        tableColFormatData.add(new ArrayList<>());
      }

      while (rs.next()) {
        for (int i = 0; i < cProfiles.size(); i++) {
          CProfile cProfile = cProfiles.get(i);
          Object cellValue = rs.getObject(cProfile.getColName());

          if (cProfile.getCsType().getCType().equals(CType.STRING)) {
            tableColFormatData.get(i).add(convertRawToString(cellValue, cProfile));
          } else {
            tableColFormatData.get(i).add(cellValue);
          }

          if (cProfiles.get(i).getColName().equals(tsCProfile.get().getColName())) {
            long value = convertRawToLong(cellValue, tsCProfile.get());
            pointerLocal.set(Map.entry(value, fetchCounter.incrementAndGet()));
          }
        }

        if (fetchCounter.get() >= fetchSize) {
          break;
        }
      }
    } catch (SQLException e) {
      log.catching(e);
    }

    if (fetchCounter.get() >= fetchSize) {
      fetchCounter.set(0);
      pointer = Map.entry(pointerLocal.get().getKey(), 0);
    } else {
      pointer = pointerLocal.get();
    }

    isStarted = false;

    if (pointer.getKey() >= maxBlockId) {
      isNext = false;
    }

    return transpose(tableColFormatData);
  }

  private String getSqlQueryTimeSeries(Optional<CProfile> tsCProfile) {
    return "SELECT * FROM " + tableName + " "
        + databaseDialect.getWhereClass(tsCProfile.get(), null, null, null)
        + databaseDialect.getOrderByClass(tsCProfile.get())
        + databaseDialect.getLimitClass(fetchSize);
  }

  private String getSqlQueryRegular() {
    if (databaseDialect instanceof OracleDialect) {
      return "SELECT * FROM " + tableName
          + " OFFSET " + currentOffset + " ROWS FETCH NEXT " + fetchSize + " ROWS ONLY ";
    }

    if (databaseDialect instanceof MsSqlDialect) {
      return "SELECT * FROM " + tableName
          + " ORDER BY (SELECT NULL) OFFSET " + currentOffset + " ROWS FETCH NEXT " + fetchSize + " ROWS ONLY ";
    }

    if (databaseDialect instanceof FirebirdDialect) {
      int start = currentOffset + 1;
      int end = currentOffset + fetchSize;
      return "SELECT * FROM " + tableName + " ROWS " + start + " TO " + end;
    }

    return "SELECT * FROM " + tableName
        + databaseDialect.getLimitClass(fetchSize)
        + databaseDialect.getOffsetClass(currentOffset);
  }

  @Override
  public boolean next() {
    if (tableType == TType.REGULAR) {
      return nextRegular();
    }
    return nextTimeSeries();
  }

  private boolean nextRegular() {
    if (!isNext) {
      return false;
    }

    if (currentOffset >= maxBlockId) {
      isNext = false;
      return false;
    }

    return true;
  }

  private boolean nextTimeSeries() {
    Optional<CProfile> tsCProfile = cProfiles.stream()
        .filter(k -> k.getCsType().isTimeStamp())
        .findAny();

    if (isStarted) {
      String query = getSqlQueryTimeSeries(tsCProfile);

      try (Connection connection = basicDataSource.getConnection();
          PreparedStatement ps = connection.prepareStatement(query)) {

        long dateTime = pointer.getKey();
        databaseDialect.setDateTime(tsCProfile.orElseThrow(), ps, 1, dateTime);
        databaseDialect.setDateTime(tsCProfile.orElseThrow(), ps, 2, maxBlockId);

        try (ResultSet rs = ps.executeQuery()) {
          if (!rs.next()) {
            log.info("Empty result set");
            isNext = false;
          } else {
            log.info("Not empty result set");
          }
        }
      } catch (SQLException e) {
        log.catching(e);
      }

      isStarted = true;
    }

    return isNext;
  }
}