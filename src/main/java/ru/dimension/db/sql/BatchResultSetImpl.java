package ru.dimension.db.sql;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.service.CommonServiceApi;
import ru.dimension.db.service.RawService;

public class BatchResultSetImpl extends CommonServiceApi implements BatchResultSet {

  private final String tableName;
  private final byte tableId;
  private final int fetchSize;
  private final List<CProfile> cProfiles;

  private final RawService rawService;

  private Map.Entry<Long, Integer> pointer;

  private boolean isNext = true;
  private boolean isStarted = true;

  private final long maxBlockId;

  private final boolean isTimestamp;

  /**
   * A ResultSet object maintains a cursor pointing to rows of data by fetchSize (local Berkley DB)
   *
   * @param tableName  table name
   * @param tableId    table id
   * @param fetchSize  the number of rows to fetch
   * @param begin      the start range
   * @param end        the end range
   * @param cProfiles  list of column profiles
   * @param rawService service layer for raw data
   */
  public BatchResultSetImpl(String tableName,
                            byte tableId,
                            int fetchSize,
                            long begin,
                            long end,
                            List<CProfile> cProfiles,
                            RawService rawService) {
    this.tableName = tableName;
    this.tableId = tableId;
    this.fetchSize = fetchSize;
    this.cProfiles = cProfiles;
    this.rawService = rawService;

    isTimestamp = cProfiles.stream().anyMatch(f -> f.getCsType().isTimeStamp());

    this.pointer = Map.entry(begin, 0);

    if (end == Long.MAX_VALUE) {
      if (isTimestamp) {
        throw new RuntimeException("Not supported API for time-series tables. Use overloaded version with begin and end parameters..");
      }

      this.maxBlockId = rawService.getMaxBlockId(tableId);
    } else {
      this.maxBlockId = end;
    }
  }

  @Override
  public List<List<Object>> getObject() {
    List<List<Object>> columnDataListLocal = new ArrayList<>();

    AtomicReference<Entry<Long, Integer>> pointerLocal
        = new AtomicReference<>(Map.entry(isStarted ? 0L : pointer.getKey(), isStarted ? 0 : pointer.getValue()));

    Optional<CProfile> tsCProfile = cProfiles.stream()
        .filter(k -> k.getCsType().isTimeStamp())
        .findAny();

    cProfiles.stream()
        .sorted(Comparator.comparing(CProfile::getColId))
        .toList()
        .forEach(cProfile -> {
          AtomicInteger fetchCounter = new AtomicInteger(fetchSize);

          Map.Entry<Map.Entry<Long, Integer>, List<Object>> columnData =
              rawService.getColumnData(tableId, cProfile.getColId(),
                                       tsCProfile.map(CProfile::getColId).orElse(-1),
                                       cProfile, fetchSize, isStarted, maxBlockId, pointer, fetchCounter);

          pointerLocal.set(columnData.getKey());

          columnDataListLocal.add(cProfile.getColId(), columnData.getValue());
        });

    pointer = pointerLocal.get();

    isStarted = false;

    if (pointer.getKey() > maxBlockId) {
      isNext = false;
    }

    return transpose(columnDataListLocal);
  }

  @Override
  public boolean next() {
    // Check if data exist in first call
    Optional<CProfile> tsCProfile = cProfiles.stream()
        .filter(k -> k.getCsType().isTimeStamp())
        .findAny();

    Optional<CProfile> anyCProfile = cProfiles.stream()
        .filter(k -> !k.getCsType().isTimeStamp())
        .findAny();

    AtomicInteger fetchCounter = new AtomicInteger(fetchSize);

    if (isStarted) {
      Map.Entry<Map.Entry<Long, Integer>, List<Object>> columnData =
          rawService.getColumnData(tableId,
                                   anyCProfile.orElseThrow().getColId(),
                                   tsCProfile.map(CProfile::getColId).orElse(-1),
                                   anyCProfile.orElseThrow(),
                                   fetchSize, isStarted, maxBlockId, pointer, fetchCounter);

      if (columnData.getValue().isEmpty()) {
        isNext = false;
      }

      isStarted = true; // Return it back
    }

    return isNext;
  }

}
