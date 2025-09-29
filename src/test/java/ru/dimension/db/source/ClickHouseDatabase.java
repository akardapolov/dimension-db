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
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
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
      tProfile = dStore.loadJdbcTableMetadata(connection, selectTest,
                                              getSProfile(tableName, tType, iType, aType, compression));
    } catch (TableNameEmptyException e) {
      throw new RuntimeException(e);
    }

    LocalDate start = LocalDate.of(2016, 1, 1);
    LocalDate end = LocalDate.of(2017, 1, 1);

    String select2016template = "SELECT * FROM datasets.trips_mergetree where toYYYYMMDD(pickup_date) = ";

    List<LocalDate> days = start.datesUntil(end).toList();
    int numBatches = (int) Math.ceil((double) days.size() / batchSize);

    // Use a fixed thread pool with limited parallelism to control memory usage
    int parallelism = Math.min(batchSize, Runtime.getRuntime().availableProcessors());

    for (int i = 0; i < numBatches; i++) {
      int startIndex = i * batchSize;
      int endIndex = Math.min((i + 1) * batchSize, days.size());

      List<LocalDate> batchDays = days.subList(startIndex, endIndex);

      List<LoadDataTask> tasks = new ArrayList<>();
      batchDays.forEach(day -> {
        tasks.add(new LoadDataTask(day, select2016template, cProfileList, cProfiles, resultSetFetchSize, dStore, tProfile));
      });

      // Use fixed parallelism and process tasks with controlled memory usage
      try (ForkJoinPool pool = new ForkJoinPool(parallelism)) {
        // Process tasks and handle results incrementally
        tasks.forEach(task -> {
          try {
            List<List<Object>> taskResult = pool.submit(task).get();
            if (taskResult != null && !taskResult.isEmpty()) {
              try {
                dStore.putDataDirect(tProfile.getTableName(), taskResult);
              } catch (SqlColMetadataException | EnumByteExceedException ex) {
                log.error("Error storing data for day {}: {}", task.getDay(), ex.getMessage(), ex);
              }
              // Explicitly clear the result to free memory
              taskResult.forEach(List::clear);
            }
          } catch (InterruptedException | ExecutionException e) {
            Thread.currentThread().interrupt();
            log.error("Error processing task for day {}: {}", task.getDay(), e.getMessage(), e);
          }
        });
      }
    }

    return cProfiles;
  }

  class LoadDataTask implements Callable<List<List<Object>>> {
    private final LocalDate day;
    private final String selectTemplate;
    private final List<CProfile> cProfiles;
    private final int resultSetFetchSize;
    private final DStore dStore;
    private final TProfile tProfile;

    public LocalDate getDay() {
      return day;
    }

    public LoadDataTask(LocalDate day,
                        String selectTemplate,
                        List<CProfile> cProfileList,
                        List<CProfile> cProfiles,
                        int resultSetFetchSize,
                        DStore dStore,
                        TProfile tProfile) {
      this.day = day;
      this.selectTemplate = selectTemplate;
      this.cProfiles = cProfiles;
      this.resultSetFetchSize = resultSetFetchSize;
      this.dStore = dStore;
      this.tProfile = tProfile;
    }

    @Override
    public List<List<Object>> call() {
      log.info("Start task for day {} at: {}", day, LocalDateTime.now());

      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
      String sDay = day.format(formatter);
      String query = selectTemplate + sDay + " ORDER BY pickup_datetime ASC";

      log.info("Executing query: {}", query);

      List<List<Object>> listsColStore = new ArrayList<>();
      cProfiles.forEach(v -> listsColStore.add(new ArrayList<>()));

      try (PreparedStatement ps = connection.prepareStatement(query)) {
        ps.setFetchSize(resultSetFetchSize);
        try (ResultSet r = ps.executeQuery()) {
          boolean hasData = false;
          int rowCount = 0;

          while (r.next()) {
            for (CProfile v : cProfiles) {
              addToList(listsColStore, v, r);
            }
            hasData = true;
            rowCount++;

            // Process in smaller chunks to avoid memory buildup
            if (rowCount % 10000 == 0) {
              log.debug("Processed {} rows for day {}", rowCount, day);
            }
          }

          if (hasData) {
            log.info("Completed processing day {} with {} rows", day, rowCount);
            return listsColStore;
          } else {
            log.info("No data found for day {}", day);
            listsColStore.forEach(List::clear);
            return null;
          }
        }
      } catch (SQLException e) {
        log.error("Error executing query for day {}: {}", day, e.getMessage(), e);
        listsColStore.forEach(List::clear);
        return null;
      }
    }

    private void addToList(List<List<Object>> lists, CProfile v, ResultSet r) throws SQLException {
      lists.get(v.getColId()).add(r.getObject(v.getColIdSql()));
    }
  }

  public List<CProfile> loadDataDirect(String select,
                                       DStore dStore,
                                       TType tType,
                                       IType iType,
                                       AType aType,
                                       Boolean compression,
                                       int resultSetFetchSize) throws SQLException {

    List<List<Object>> listsColStore = new ArrayList<>();
    String selectTest = select + " LIMIT 1";
    List<CProfile> cProfileList = loadSqlColMetadataList(selectTest);

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
      tProfile = dStore.loadJdbcTableMetadata(connection, selectTest,
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

  public void close() throws SQLException {
    connection.close();
  }
}