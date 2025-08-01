package ru.dimension.db.source;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import ru.dimension.db.core.DStore;
import ru.dimension.db.exception.EnumByteExceedException;
import ru.dimension.db.exception.SqlColMetadataException;
import ru.dimension.db.exception.TableNameEmptyException;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.TProfile;
import ru.dimension.db.model.profile.cstype.CSType;
import ru.dimension.db.model.profile.table.AType;
import ru.dimension.db.model.profile.table.IType;
import ru.dimension.db.model.profile.table.TType;
import ru.dimension.db.service.mapping.Mapper;

@Log4j2
public class ClickHouseDatabase implements ClickHouse {
  private final Connection connection;

  @Getter
  private TProfile tProfile;

  @Getter
  private final List<CProfile> cProfileList = new ArrayList<>();

  public enum Step {
    DAY, HOUR
  }

  public ClickHouseDatabase(String url) throws SQLException {
    connection = DriverManager.getConnection(url);
  }

  public List<CProfile> loadDataDirectParallel(String select,
                                               DStore dStore,
                                               TType tType,
                                               IType iType,
                                               AType aType,
                                               Boolean compression,
                                               int batchSize,
                                               int resultSetFetchSize) throws SQLException {
    List<CProfile> cProfileList = loadSqlColMetadataList(select);

    List<CProfile> cProfiles = cProfileList.stream()
        .map(cProfile -> cProfile.toBuilder()
            .colId(cProfile.getColId())
            .colName(cProfile.getColName())
            .colDbTypeName(cProfile.getColDbTypeName())
            .colSizeDisplay(cProfile.getColSizeDisplay())
            .csType(CSType.builder()
                        .isTimeStamp(cProfile.getColName().equalsIgnoreCase("PICKUP_DATETIME"))
                        .sType(getSType(cProfile.getColName()))
                        .cType(Mapper.isCType(cProfile))
                        .dType(Mapper.isDBType(cProfile))
                        .build())
            .build()).toList();

    try {
      tProfile = dStore.loadJdbcTableMetadata(connection, select,
                                              getSProfile(tableName, tType, iType, aType, compression));
    } catch (TableNameEmptyException e) {
      throw new RuntimeException(e);
    }

    LocalDate start = LocalDate.of(2016, 1, 1);
    LocalDate end = LocalDate.of(2017, 1, 1);

    String select2016template = "SELECT * FROM datasets.trips_mergetree where toYYYYMMDD(pickup_date) = ";

    List<LocalDate> days = start.datesUntil(end).collect(Collectors.toList());
    int numBatches = (int) Math.ceil((double) days.size() / batchSize);

    for (int i = 0; i < numBatches; i++) {
      int startIndex = i * batchSize;
      int endIndex = Math.min((i + 1) * batchSize, days.size());

      List<LocalDate> batchDays = days.subList(startIndex, endIndex);

      List<LoadDataTask> tasks = new ArrayList<>();

      batchDays.forEach(day -> {
        tasks.add(new LoadDataTask(day, select2016template, cProfileList, cProfiles, resultSetFetchSize));
      });

      ForkJoinPool pool = new ForkJoinPool();

      for (LoadDataTask task : tasks) {
        pool.execute(task);
      }

      pool.shutdown();

      while (!pool.isTerminated()) {
        // 1. Implement additional logic here to handle any pseudo "join" operation
        // 2. Wait while ForkJoinPool will be terminated
      }

      tasks.forEach(task -> {
        if (task.getIsDataExist().get()) {
          try {
            dStore.putDataDirect(tProfile.getTableName(), task.getListsColStore());
          } catch (SqlColMetadataException | EnumByteExceedException ex) {
            throw new RuntimeException(ex);
          }
        }
      });
    }

    return cProfiles;
  }

  class LoadDataTask extends RecursiveAction {
    private final LocalDate day;
    private final String selectTemplate;
    private final List<CProfile> cProfileList;
    private final List<CProfile> cProfiles;
    private final int resultSetFetchSize;

    @Getter
    private final AtomicBoolean isDataExist = new AtomicBoolean(false);

    @Getter
    private final List<List<Object>> listsColStore = new ArrayList<>();

    public LoadDataTask(LocalDate day,
                        String selectTemplate,
                        List<CProfile> cProfileList,
                        List<CProfile> cProfiles,
                        int resultSetFetchSize) {
      this.day = day;

      this.selectTemplate = selectTemplate;
      this.cProfileList = cProfileList;
      this.cProfiles = cProfiles;
      this.resultSetFetchSize = resultSetFetchSize;
    }

    @Override
    protected void compute() {
      log.info("Start task at: " + LocalDateTime.now());

      cProfileList.forEach(v -> listsColStore.add(new ArrayList<>()));

      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
      String sDay = day.format(formatter);
      String query = selectTemplate + sDay + " ORDER BY pickup_datetime ASC";

      log.info("Start execution query: " + query);
      try (PreparedStatement ps = connection.prepareStatement(query)) {
        ps.setFetchSize(resultSetFetchSize);
        try (ResultSet r = ps.executeQuery()) {
          while (r.next()) {
            cProfiles.forEach(v -> {
              try {
                addToList(listsColStore, v, r);
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            });
            isDataExist.set(true);
          }
        }
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }

      log.info("End task at: " + LocalDateTime.now());
    }
  }

  public List<CProfile> loadDataDirect(String select,
                                       DStore dStore,
                                       TType tType,
                                       IType iType,
                                       AType aType,
                                       Boolean compression,
                                       int resultSetFetchSize,
                                       boolean saveMetadataAndExit) throws SQLException {

    List<List<Object>> listsColStore = new ArrayList<>();
    List<CProfile> cProfileList = loadSqlColMetadataList(select);

    cProfileList.forEach(v -> listsColStore.add(v.getColId(), new ArrayList<>()));

    List<CProfile> cProfiles = cProfileList.stream()
        .map(cProfile -> cProfile.toBuilder()
            .colId(cProfile.getColId())
            .colName(cProfile.getColName())
            .colDbTypeName(cProfile.getColDbTypeName())
            .colSizeDisplay(cProfile.getColSizeDisplay())
            .csType(CSType.builder()
                        .isTimeStamp(cProfile.getColName().equalsIgnoreCase("PICKUP_DATETIME"))
                        .sType(getSType(cProfile.getColName()))
                        .cType(Mapper.isCType(cProfile))
                        .dType(Mapper.isDBType(cProfile))
                        .build())
            .build()).toList();

    try {
      tProfile = dStore.loadJdbcTableMetadata(connection, select,
                                              getSProfile(tableName, tType, iType, aType, compression));
    } catch (TableNameEmptyException e) {
      throw new RuntimeException(e);
    }

    cProfileList.forEach(v -> listsColStore.add(v.getColId(), new ArrayList<>()));

    LocalDate start = LocalDate.of(2016, 1, 1);
    LocalDate end = LocalDate.of(2017, 1, 1);

    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");

    String select2016template = "SELECT * FROM datasets.trips_mergetree where toYYYYMMDD(pickup_date) = ";

    Iterable<LocalDate> dates = () -> start.datesUntil(end).iterator();
    List<LocalDate> dateList = StreamSupport.stream(dates.spliterator(), false).toList();

    for (int i = 0; i < dateList.size(); i++) {
      String sDay = dateList.get(i).format(formatter);

      try {
        String query = select2016template + sDay + " ORDER BY pickup_datetime ASC";

        log.info("Start query: " + query);
        PreparedStatement ps = connection.prepareStatement(query);
        ps.setFetchSize(resultSetFetchSize);
        ResultSet r = ps.executeQuery();

        AtomicBoolean isDataExist = new AtomicBoolean(false);
        while (r.next()) {
          cProfiles.forEach(v -> {
            try {
              addToList(listsColStore, v, r);
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
          });

          //////////////////////////
          if (saveMetadataAndExit) {
            try {
              storeResultSetDataToFile(listsColStore);

              break;
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }

          isDataExist.set(true);
        }

        r.close();
        ps.close();

        if (isDataExist.get()) {
          dStore.putDataDirect(tProfile.getTableName(), listsColStore);
        }

        log.info("End query: " + query);
      } catch (Exception e) {
        log.catching(e);
      }

      listsColStore.clear();
      cProfileList.forEach(v -> listsColStore.add(v.getColId(), new ArrayList<>()));

      //////////////////////////
      if (saveMetadataAndExit) {
        break;
      }
    }

    return cProfiles;
  }

  public List<CProfile> loadDataJdbc(String select,
                                     DStore dStore,
                                     TType tType,
                                     IType iType,
                                     AType aType,
                                     Boolean compression,
                                     int resultSetFetchSize,
                                     LocalDate startDate,
                                     LocalDate endDate,
                                     Step step) throws SQLException {

    log.info("Start time: " + LocalDateTime.now());

    String selectTest = select + " LIMIT 1";

    List<CProfile> cProfileList = loadSqlColMetadataList(selectTest);

    List<CProfile> cProfiles = cProfileList.stream()
        .map(cProfile -> cProfile.toBuilder()
            .colId(cProfile.getColId())
            .colName(cProfile.getColName())
            .colDbTypeName(cProfile.getColDbTypeName())
            .colSizeDisplay(cProfile.getColSizeDisplay())
            .csType(CSType.builder()
                        .isTimeStamp(cProfile.getColName().equalsIgnoreCase("PICKUP_DATETIME"))
                        .sType(getSType(cProfile.getColName()))
                        .cType(Mapper.isCType(cProfile))
                        .dType(Mapper.isDBType(cProfile))
                        .build())
            .build()).toList();

    try {
      tProfile = dStore.loadJdbcTableMetadata(connection, selectTest, getSProfile(tableName, tType, iType, aType, compression));
    } catch (TableNameEmptyException e) {
      throw new RuntimeException(e);
    }

    DateTimeFormatter dayFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
    DateTimeFormatter hourFormatter = DateTimeFormatter.ofPattern("yyyyMMddHH");

    long stepCount;
    switch (step) {
      case DAY -> stepCount = ChronoUnit.DAYS.between(startDate, endDate);
      case HOUR -> stepCount = ChronoUnit.HOURS.between(
          startDate.atStartOfDay(),
          endDate.atStartOfDay()
      );
      default -> throw new IllegalArgumentException("Unsupported step: " + step);
    }

    for (long i = 0; i < stepCount; i++) {
      String condition;
      if (step == Step.DAY) {
        LocalDate currentDate = startDate.plusDays(i);
        condition = "toYYYYMMDD(pickup_date) = " + currentDate.format(dayFormatter);
      } else {
        LocalDateTime currentDateTime = startDate.atStartOfDay().plusHours(i);
        condition = "toYYYYMMDDhhmmss(pickup_datetime) BETWEEN " +
            currentDateTime.format(hourFormatter) + "0000 AND " +
            currentDateTime.format(hourFormatter) + "5959";
      }

      try {
        String query = select + " WHERE " + condition + " ORDER BY pickup_datetime ASC";

        log.info("Start query: " + query);
        try (PreparedStatement ps = connection.prepareStatement(query)) {
          ps.setFetchSize(resultSetFetchSize);
          try (ResultSet r = ps.executeQuery()) {
            dStore.putDataJdbc(tProfile.getTableName(), r);
          }
        }
        log.info("End query: " + query);
      } catch (Exception e) {
        log.catching(e);
      }
    }

    log.info("End time: " + LocalDateTime.now());
    return cProfiles;
  }

  public List<CProfile> loadDataJdbcBatch(String select,
                                          DStore dStore,
                                          TType tType,
                                          IType iType,
                                          AType aType,
                                          Boolean compression,
                                          int fBaseBatchSize,
                                          int resultSetFetchSize) throws SQLException, EnumByteExceedException, SqlColMetadataException {

    log.info("Start time: " + LocalDateTime.now());

    List<CProfile> cProfileList = loadSqlColMetadataList(select);

    List<CProfile> cProfiles = cProfileList.stream()
        .map(cProfile -> cProfile.toBuilder()
            .colId(cProfile.getColId())
            .colName(cProfile.getColName())
            .colDbTypeName(cProfile.getColDbTypeName())
            .colSizeDisplay(cProfile.getColSizeDisplay())
            .csType(CSType.builder()
                        .isTimeStamp(cProfile.getColName().equalsIgnoreCase("PICKUP_DATETIME"))
                        .sType(getSType(cProfile.getColName()))
                        .cType(Mapper.isCType(cProfile))
                        .dType(Mapper.isDBType(cProfile))
                        .build())
            .build()).toList();

    try {
      tProfile = dStore.loadJdbcTableMetadata(connection, select, getSProfile(tableName, tType, iType, aType, compression));
    } catch (TableNameEmptyException e) {
      throw new RuntimeException(e);
    }

    PreparedStatement ps = connection.prepareStatement(select);
    ps.setFetchSize(resultSetFetchSize);
    ResultSet r = ps.executeQuery();

    dStore.putDataJdbcBatch(tProfile.getTableName(), r, fBaseBatchSize);

    r.close();
    ps.close();

    log.info("End time: " + LocalDateTime.now());
    return cProfiles;
  }

  private void addToList(List<List<Object>> lists, CProfile v, ResultSet r ) throws SQLException {
    lists.get(v.getColId()).add(r.getObject(v.getColIdSql()));
  }

  private void storeObjectToFile(Object objectToStore, String fileName) throws IOException {
    Path filePath = Paths.get("./src/test/resources/clickhouse", fileName);
    Path absolutePath = filePath.toAbsolutePath().normalize();

    try (FileOutputStream fos = new FileOutputStream(absolutePath.toFile());
        ObjectOutputStream oos = new ObjectOutputStream(fos)) {
      oos.writeObject(objectToStore);
    }
  }

  private void storeResultSetDataToFile(List<List<Object>> listsColStore) throws IOException {
    String dbFolder = getTestDbFolder();
    Path sourceMetamodel = Paths.get(dbFolder, "metamodel.obj");
    Path destinationMetamodel = Paths.get("./src/test/resources/clickhouse", "metamodel.obj");

    if (Files.exists(sourceMetamodel)) {
      Files.copy(sourceMetamodel, destinationMetamodel, StandardCopyOption.REPLACE_EXISTING);
    } else {
      throw new RuntimeException("File not found: " + sourceMetamodel);
    }

    storeObjectToFile(listsColStore, "listsColStore.obj");
  }

  private void storeCProfileListToFile(List<CProfile> cProfileList) throws IOException {
    storeObjectToFile(cProfileList, "sqlColProfileList.obj");
  }

  public List<CProfile> loadSqlColMetadataList(String select) throws SQLException {
    Statement s;
    ResultSet rs;
    ResultSetMetaData rsmd;

    s = connection.createStatement();
    s.executeQuery(select);
    rs = s.getResultSet();
    rsmd = rs.getMetaData();

    for (int i = 1; i <= rsmd.getColumnCount(); i++) {
      cProfileList.add(i - 1,
                       CProfile.builder()
                           .colId(i-1)
                           .colIdSql(i)
                           .colName(rsmd.getColumnName(i).toUpperCase())

                           .colDbTypeName(rsmd.getColumnTypeName(i).toUpperCase().contains("(") ?
                                              rsmd.getColumnTypeName(i).toUpperCase().substring(0, rsmd.getColumnTypeName(i).toUpperCase().indexOf("("))
                                              : rsmd.getColumnTypeName(i).toUpperCase())

                           .colSizeDisplay(rsmd.getColumnDisplaySize(i))
                           .build());
    }

    rs.close();
    s.close();

    return cProfileList;
  }

  public long[] getMinMaxTimestampMillis(LocalDate dateFrom, LocalDate dateTo) throws SQLException {
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
    String dateStrFrom = dateFrom.format(formatter);
    String dateStrTo = dateTo.format(formatter);
    String query = "SELECT " +
        "toUnixTimestamp(min(pickup_datetime)) * 1000, " +
        "toUnixTimestamp(max(pickup_datetime)) * 1000 " +
        "FROM datasets.trips_mergetree " +
        "WHERE toYYYYMMDD(pickup_datetime) BETWEEN " + dateStrFrom + " AND " + dateStrTo;

    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(query)) {
      if (rs.next()) {
        return new long[]{rs.getLong(1), rs.getLong(2)};
      } else {
        throw new SQLException("No results found for date: " + dateFrom + " - " + dateStrTo);
      }
    }
  }

  public void close() throws SQLException {
    connection.close();
  }
}