package ru.dimension.db.service.impl;

import com.sleepycat.persist.EntityCursor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.log4j.Log4j2;
import ru.dimension.db.core.metamodel.MetaModelApi;
import ru.dimension.db.exception.SqlColMetadataException;
import ru.dimension.db.model.output.BlockKeyTail;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.cstype.SType;
import ru.dimension.db.model.profile.table.BType;
import ru.dimension.db.service.CommonServiceApi;
import ru.dimension.db.service.RawService;
import ru.dimension.db.sql.BatchResultSet;
import ru.dimension.db.sql.BatchResultSetImpl;
import ru.dimension.db.storage.Converter;
import ru.dimension.db.storage.EnumDAO;
import ru.dimension.db.storage.HistogramDAO;
import ru.dimension.db.storage.RawDAO;
import ru.dimension.db.storage.bdb.entity.Metadata;
import ru.dimension.db.storage.bdb.entity.MetadataKey;
import ru.dimension.db.storage.bdb.entity.column.EColumn;
import ru.dimension.db.storage.helper.EnumHelper;

@Log4j2
public class RawServiceImpl extends CommonServiceApi implements RawService {
  private final MetaModelApi metaModelApi;
  private final Converter converter;
  private final RawDAO rawDAO;
  private final HistogramDAO histogramDAO;
  private final EnumDAO enumDAO;

  public RawServiceImpl(MetaModelApi metaModelApi,
                        Converter converter,
                        RawDAO rawDAO,
                        HistogramDAO histogramDAO,
                        EnumDAO enumDAO) {
    this.metaModelApi = metaModelApi;
    this.converter = converter;
    this.rawDAO = rawDAO;
    this.histogramDAO = histogramDAO;
    this.enumDAO = enumDAO;
  }

  @Override
  public List<List<Object>> getRawDataAll(String tableName,
                                          long begin,
                                          long end) {
    List<CProfile> cProfiles = metaModelApi.getCProfiles(tableName);
    return getRawData(tableName, cProfiles, begin, end);
  }

  @Override
  public List<List<Object>> getRawDataAll(String tableName,
                                          CProfile cProfileFilter,
                                          String filter,
                                          long begin,
                                          long end) {
    List<CProfile> cProfiles = metaModelApi.getCProfiles(tableName);
    return getRawData(tableName, cProfiles, cProfileFilter, filter, begin, end);
  }

  @Override
  public List<List<Object>> getRawDataByColumn(String tableName,
                                               CProfile cProfile,
                                               long begin,
                                               long end) {
    CProfile tsProfile = metaModelApi.getTimestampCProfile(tableName);
    List<CProfile> cProfiles = List.of(tsProfile, cProfile);
    return getRawData(tableName, cProfiles, begin, end);
  }

  @Override
  public BatchResultSet getBatchResultSet(String tableName,
                                          long begin,
                                          long end,
                                          int fetchSize) {
    byte tableId = metaModelApi.getTableId(tableName);
    BType bType = metaModelApi.getBackendType(tableName);
    List<CProfile> cProfiles = metaModelApi.getCProfiles(tableName);

    if (BType.BERKLEYDB.equals(bType)) {
      return new BatchResultSetImpl(tableName, tableId, fetchSize, begin, end, cProfiles, this);
    } else {
      return rawDAO.getBatchResultSet(tableName, begin, end, fetchSize, cProfiles);
    }
  }

  @Override
  public Entry<Entry<Long, Integer>, List<Object>> getColumnData(byte tableId,
                                                                 int colId,
                                                                 int tsColId,
                                                                 CProfile cProfile,
                                                                 int fetchSize,
                                                                 boolean isStarted,
                                                                 long maxBlockId,
                                                                 Entry<Long, Integer> pointer,
                                                                 AtomicInteger fetchCounter) {

    List<Object> columnData = new ArrayList<>();

    boolean isPointerFirst = true;
    boolean getNextPointer = false;

    if (tsColId != -1) {
      long prevBlockId = this.rawDAO.getPreviousBlockId(tableId, pointer.getKey());
      if (prevBlockId != pointer.getKey() & prevBlockId != 0) {
        isStarted = false;

        long[] timestamps = rawDAO.getRawLong(tableId, prevBlockId, tsColId);

        for (int i = 0; i < timestamps.length; i++) {
          if (timestamps[i] == pointer.getKey()) {
            pointer = Map.entry(prevBlockId, i);
          }
        }
      }
    }

    MetadataKey beginMK = MetadataKey.builder().tableId(tableId).blockId(pointer.getKey()).build();
    MetadataKey endMK = MetadataKey.builder().tableId(tableId).blockId(maxBlockId).build();
    EntityCursor<Metadata> cursor = rawDAO.getMetadataEntityCursor(beginMK, endMK);

    try (cursor) {
      Metadata columnKey;

      while ((columnKey = cursor.next()) != null) {
        long blockId = columnKey.getMetadataKey().getBlockId();

        if (getNextPointer) {
          return Map.entry(Map.entry(blockId, 0), columnData);
        }

        if (cProfile.getCsType().isTimeStamp()) { // timestamp
          int startPoint = isStarted ? 0 : isPointerFirst ? pointer.getValue() : 0;

          long[] timestamps = rawDAO.getRawLong(tableId, blockId, cProfile.getColId());

          for (int i = startPoint; i < timestamps.length; i++) {
            columnData.add(timestamps[i]);
            fetchCounter.decrementAndGet();
            if (fetchCounter.get() == 0) {
              if (i == timestamps.length - 1) {
                getNextPointer = true;
              } else {
                return Map.entry(Map.entry(blockId, i + 1), columnData);
              }
            }
          }

          if (isPointerFirst) {
            isPointerFirst = false;
          }
        } else {
          SType sType = getSType(colId, columnKey);

          if (SType.RAW.equals(sType) & !cProfile.getCsType().isTimeStamp()) { // raw data
            int startPoint = isStarted ? 0 : isPointerFirst ? pointer.getValue() : 0;

            String[] column = getStringArrayValuesRaw(rawDAO, tableId, blockId, cProfile);

            for (int i = startPoint; i < column.length; i++) {
              columnData.add(column[i]);
              fetchCounter.decrementAndGet();
              if (fetchCounter.get() == 0) {
                if (i == column.length - 1) {
                  getNextPointer = true;
                } else {
                  return Map.entry(Map.entry(blockId, i + 1), columnData);
                }
              }
            }

            if (isPointerFirst) {
              isPointerFirst = false;
            }
          }

          if (SType.HISTOGRAM.equals(sType)) { // indexed data
            long[] timestamps = rawDAO.getRawLong(tableId, blockId, tsColId);

            int[][] h = histogramDAO.get(tableId, blockId, cProfile.getColId());

            int startPoint = isStarted ? 0 : isPointerFirst ? pointer.getValue() : 0;

            for (int i = startPoint; i < timestamps.length; i++) {
              columnData.add(this.converter.convertIntToRaw(getHistogramValue(i, h, timestamps), cProfile));
              fetchCounter.decrementAndGet();
              if (fetchCounter.get() == 0) {
                if (i == timestamps.length - 1) {
                  getNextPointer = true;
                } else {
                  return Map.entry(Map.entry(blockId, i + 1), columnData);
                }
              }
            }

            if (isPointerFirst) {
              isPointerFirst = false;
            }
          }

          if (SType.ENUM.equals(sType)) { // enum data
            long[] timestamps = rawDAO.getRawLong(tableId, blockId, tsColId);

            EColumn eColumn = enumDAO.getEColumnValues(tableId, blockId, cProfile.getColId());

            int startPoint = isStarted ? 0 : isPointerFirst ? pointer.getValue() : 0;

            for (int i = startPoint; i < timestamps.length; i++) {
              columnData.add(converter.convertIntToRaw(EnumHelper.getIndexValue(eColumn.getValues(), eColumn.getDataByte()[i]), cProfile));
              fetchCounter.decrementAndGet();
              if (fetchCounter.get() == 0) {
                if (i == timestamps.length - 1) {
                  getNextPointer = true;
                } else {
                  return Map.entry(Map.entry(blockId, i + 1), columnData);
                }
              }
            }

            if (isPointerFirst) {
              isPointerFirst = false;
            }
          }
        }

      }
    } catch (Exception e) {
      log.error(e.getMessage());
      log.catching(e);
    }

    cursor.close();

    return Map.entry(Map.entry(maxBlockId + 1, 0), columnData);
  }

  @Override
  public long getMaxBlockId(byte tableId) {
    return rawDAO.getLastBlockId(tableId);
  }

  @Override
  public long getFirst(String tableName,
                       long begin,
                       long end) {
    byte tableId = metaModelApi.getTableId(tableName);
    int tsColId = metaModelApi.getTimestampCProfile(tableName).getColId();

    return this.rawDAO.getFirstBlockId(tableId, tsColId, begin, end);
  }

  @Override
  public long getLast(String tableName,
                      long begin,
                      long end) {
    byte tableId = metaModelApi.getTableId(tableName);
    int tsColId = metaModelApi.getTimestampCProfile(tableName).getColId();

    return this.rawDAO.getLastBlockId(tableId, tsColId, begin, end);
  }

  @Override
  public List<BlockKeyTail> getBlockKeyTailList(String tableName,
                                               long begin,
                                               long end) throws SqlColMetadataException {
    byte tableId = metaModelApi.getTableId(tableName);

    CProfile tsProfile = metaModelApi.getTimestampCProfile(tableName);

    if (!tsProfile.getCsType().isTimeStamp()) {
      throw new SqlColMetadataException("Timestamp column not defined..");
    }

    long previousBlockId = this.rawDAO.getPreviousBlockId(tableId, begin);
    Map.Entry<MetadataKey, MetadataKey> keyEntry = getMetadataKeyPair(tableId, begin, end, previousBlockId);

    List<BlockKeyTail> blockKeyTails = new ArrayList<>();

    try (EntityCursor<Metadata> cursor = rawDAO.getMetadataEntityCursor(keyEntry.getKey(), keyEntry.getValue())) {
      Metadata columnKey;

      while ((columnKey = cursor.next()) != null) {
        long blockId = columnKey.getMetadataKey().getBlockId();

        long[] timestamps = rawDAO.getRawLong(tableId, blockId, tsProfile.getColId());

        long tail = timestamps[timestamps.length - 1];

        if (blockId >= begin & tail <= end) {
          blockKeyTails.add(new BlockKeyTail(blockId, tail));
        } else {
          if (blockId >= begin & tail > end) {
            for (int i = timestamps.length - 1; i >= 0; i--) {
              if (timestamps[i] <= end) {
                blockKeyTails.add(new BlockKeyTail(blockId, timestamps[i]));
                break;
              }
            }
          }
          if (blockId < begin & tail <= end) {
            for (long timestamp : timestamps) {
              if (timestamp >= begin) {
                blockKeyTails.add(new BlockKeyTail(timestamp, tail));
                break;
              }
            }
          }
        }
      }
    }

    return blockKeyTails;
  }

  private List<List<Object>> getRawData(String tableName,
                                        List<CProfile> cProfiles,
                                        long begin,
                                        long end) {
    byte tableId = metaModelApi.getTableId(tableName);
    CProfile tsProfile = metaModelApi.getTimestampCProfile(tableName);

    List<List<Object>> columnDataList = new ArrayList<>();

    long previousBlockId = this.rawDAO.getPreviousBlockId(tableId, begin);
    if (previousBlockId != begin & previousBlockId != 0) {
      this.computeRawDataBeginEnd(tableId, tsProfile, cProfiles, previousBlockId, begin, end, columnDataList);
    }

    this.rawDAO.getListBlockIds(tableId, begin, end)
        .forEach(blockId ->
                     this.computeRawDataBeginEnd(tableId, tsProfile, cProfiles, blockId, begin, end, columnDataList));

    return columnDataList;
  }

  private List<List<Object>> getRawData(String tableName,
                                        List<CProfile> cProfiles,
                                        CProfile cProfileFilter,
                                        String filter,
                                        long begin,
                                        long end) {
    byte tableId = metaModelApi.getTableId(tableName);
    CProfile tsProfile = metaModelApi.getTimestampCProfile(tableName);

    List<List<Object>> columnDataList = new ArrayList<>();

    long previousBlockId = this.rawDAO.getPreviousBlockId(tableId, begin);
    if (previousBlockId != begin & previousBlockId != 0) {
      this.computeRawDataBeginEnd(tableId, tsProfile, cProfiles, cProfileFilter, filter, previousBlockId, begin, end, columnDataList);
    }

    this.rawDAO.getListBlockIds(tableId, begin, end)
        .forEach(blockId ->
                     this.computeRawDataBeginEnd(tableId, tsProfile, cProfiles, cProfileFilter, filter, blockId, begin, end, columnDataList));

    return columnDataList;
  }

  private void computeRawDataBeginEnd(byte tableId,
                                      CProfile tsProfile,
                                      List<CProfile> cProfiles,
                                      long blockId,
                                      long begin,
                                      long end,
                                      List<List<Object>> columnDataListOut) {
    List<List<Object>> columnDataListLocal = new ArrayList<>();

    long[] timestamps = rawDAO.getRawLong(tableId, blockId, tsProfile.getColId());

    cProfiles.forEach(cProfile -> {

      List<Object> columnData = new ArrayList<>();

      if (cProfile.getCsType().isTimeStamp()) { // timestamp
        fillTimeSeriesColumnData(timestamps, begin, end, columnData);

        columnDataListLocal.add(cProfiles.size() == 2 ? 0 : cProfile.getColId(), columnData);
      } else {

        MetadataKey metadataKey = MetadataKey.builder().tableId(tableId).blockId(blockId).build();

        SType sType = getSType(cProfile.getColId(), rawDAO.getMetadata(metadataKey));

        if (SType.ENUM.equals(sType) || SType.HISTOGRAM.equals(sType)) {
          // Fix for BUG to pass DType to convertIntToRaw for ENUM and HISTOGRAM
          cProfile.setColDbTypeName(cProfile.getCsType().getDType().getValue());
        }

        String[] columValues = getStringArrayValues(rawDAO, enumDAO, histogramDAO, converter, tableId, cProfile, blockId, timestamps);

        fillColumnData(columValues, timestamps, begin, end, columnData);

        columnDataListLocal.add(cProfiles.size() == 2 ? 1 : cProfile.getColId(), columnData);
      }

    });

    columnDataListOut.addAll(transpose(columnDataListLocal));
  }

  private void computeRawDataBeginEnd(byte tableId,
                                      CProfile tsProfile,
                                      List<CProfile> cProfiles,
                                      CProfile cProfileFilter,
                                      String filter,
                                      long blockId,
                                      long begin,
                                      long end,
                                      List<List<Object>> columnDataListOut) {
    List<List<Object>> columnDataListLocal = new ArrayList<>();

    long[] timestamps = rawDAO.getRawLong(tableId, blockId, tsProfile.getColId());

    String[] columValuesFilter = getStringArrayValues(rawDAO, enumDAO, histogramDAO, converter, tableId, cProfileFilter, blockId, timestamps);

    Set<Integer> indexSetFilter = getIndexSetByFilter(columValuesFilter, cProfileFilter, filter, timestamps, begin, end);

    if (indexSetFilter.isEmpty()) {
      columnDataListOut.addAll(transpose(columnDataListLocal));
      return;
    }

    boolean isAllValuesFiltered = indexSetFilter.size() == columValuesFilter.length;

    cProfiles.forEach(cProfile -> {

      List<Object> columnData = new ArrayList<>();

      if (cProfile.getCsType().isTimeStamp()) { // timestamp

        if (isAllValuesFiltered) {
          fillTimeSeriesColumnData(timestamps, begin, end, columnData);
        } else {
          fillTimeSeriesColumnDataFilter(indexSetFilter, timestamps, columnData);
        }

        columnDataListLocal.add(cProfiles.size() == 2 ? 0 : cProfile.getColId(), columnData);
      } else {

        MetadataKey metadataKey = MetadataKey.builder().tableId(tableId).blockId(blockId).build();

        SType sType = getSType(cProfile.getColId(), rawDAO.getMetadata(metadataKey));

        if (SType.ENUM.equals(sType) || SType.HISTOGRAM.equals(sType)) {
          // Fix for BUG to pass DType to convertIntToRaw for ENUM and HISTOGRAM
          cProfile.setColDbTypeName(cProfile.getCsType().getDType().getValue());
        }

        String[] columValues = getStringArrayValues(rawDAO, enumDAO, histogramDAO, converter, tableId, cProfile, blockId, timestamps);

        if (isAllValuesFiltered) {
          fillColumnData(columValues, timestamps, begin, end, columnData);
        } else {
          fillColumnDataFilter(columValues, indexSetFilter, timestamps, columnData);
        }

        columnDataListLocal.add(cProfiles.size() == 2 ? 1 : cProfile.getColId(), columnData);
      }

    });

    columnDataListOut.addAll(transpose(columnDataListLocal));
  }
}
