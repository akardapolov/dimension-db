package ru.dimension.db.service.impl;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import lombok.extern.log4j.Log4j2;
import ru.dimension.db.core.metamodel.MetaModelApi;
import ru.dimension.db.exception.EnumByteExceedException;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.cstype.CSType;
import ru.dimension.db.model.profile.cstype.CType;
import ru.dimension.db.model.profile.cstype.SType;
import ru.dimension.db.model.profile.table.IType;
import ru.dimension.db.service.CommonServiceApi;
import ru.dimension.db.service.StatisticsService;
import ru.dimension.db.service.StoreLocalService;
import ru.dimension.db.service.StoreService;
import ru.dimension.db.service.mapping.Mapper;
import ru.dimension.db.service.store.EStore;
import ru.dimension.db.service.store.HEntry;
import ru.dimension.db.service.store.RStore;
import ru.dimension.db.service.store.TStore;
import ru.dimension.db.storage.Converter;
import ru.dimension.db.storage.EnumDAO;
import ru.dimension.db.storage.HistogramDAO;
import ru.dimension.db.storage.RawDAO;

@Log4j2
public class StoreServiceImpl extends CommonServiceApi implements StoreService {

  private final MetaModelApi metaModelApi;

  private final Converter converter;

  private final EnumDAO enumDAO;

  private final RawDAO rawDAO;

  private final HistogramDAO histogramDAO;

  private final StatisticsService statisticsService;
  private final StoreLocalService storeLocalService;

  public StoreServiceImpl(MetaModelApi metaModelApi,
                          StatisticsService statisticsService,
                          Converter converter,
                          RawDAO rawDAO,
                          EnumDAO enumDAO,
                          HistogramDAO histogramDAO) {
    this.metaModelApi = metaModelApi;
    this.statisticsService = statisticsService;
    this.converter = converter;

    this.rawDAO = rawDAO;
    this.enumDAO = enumDAO;
    this.histogramDAO = histogramDAO;

    this.storeLocalService = new StoreLocalServiceImpl(metaModelApi,
                                                       statisticsService,
                                                       converter,
                                                       rawDAO,
                                                       enumDAO,
                                                       histogramDAO);
  }

  @Override
  public void putDataDirect(String tableName,
                            List<List<Object>> data) {
    int rowCount = data.get(0).size();
    byte tableId = metaModelApi.getTableId(tableName);
    boolean compression = metaModelApi.getTableCompression(tableName);

    List<CProfile> cProfiles = metaModelApi.getCProfiles(tableName);
    int colCount = cProfiles.size();

    Map<Integer, SType> colIdSTypeMap = new HashMap<>();

    cProfiles.stream().filter(isNotTimestamp)
        .forEach(e -> colIdSTypeMap.put(e.getColId(), e.getCsType().getSType()));

    /* Timestamp */
    TStore tStore = new TStore(1, cProfiles);

    /* Raw */
    RStore rStore = new RStore(converter, cProfiles, colIdSTypeMap);

    /* Enum */
    EStore eStore = new EStore(cProfiles, colIdSTypeMap);

    /* Histogram */
    Map<Integer, HEntry> histograms = new HashMap<>();
    cProfiles.stream()
        .filter(isNotTimestamp)
        .forEach(e -> histograms.put(e.getColId(), new HEntry(new ArrayList<>(), new ArrayList<>())));

    List<Integer> iColumn = IntStream.range(0, colCount).boxed().toList();

    IntStream iRow = IntStream.range(0, rowCount);

    iRow.forEach(iR -> iColumn.forEach(iC -> {
      CProfile cProfile = cProfiles.get(iC);
      CSType csType = cProfile.getCsType();
      int colId = cProfile.getColId();

      Object currObject = data.get(iC).get(iR);

      // Fill timestamps
      if (csType.isTimeStamp()) {
        if (csType.getSType() == SType.RAW) {
          tStore.add(iC, iR, this.converter.getKeyValue(currObject, cProfile));
        }
      }

      // Fill raw data
      if (csType.getSType() == SType.RAW) {
        if (!csType.isTimeStamp()) {
          rStore.add(cProfile, iR, currObject);
        }
      }

      // Fill enum data
      if (csType.getSType() == SType.ENUM) {
        int curValue = this.converter.convertRawToInt(currObject, cProfile);
        int iMapping = eStore.getMapping().get(colId);
        try {
          eStore.add(iMapping, iR, curValue);
        } catch (EnumByteExceedException e) {
          throw new RuntimeException(e);
        }
      }

      // Fill histogram data
      if (csType.getSType() == SType.HISTOGRAM) {
        int prevValue = getPrevValue(histograms, colId);
        int currValue = this.converter.convertRawToInt(currObject, cProfile);

        if (prevValue != currValue) {
          histograms.get(colId).getIndex().add(iR);
          histograms.get(colId).getValue().add(currValue);
        } else if (iR == 0) {
          histograms.get(colId).getIndex().add(iR);
          histograms.get(colId).getValue().add(currValue);
        }
      }

    }));

    long blockId = tStore.getBlockId();

    this.storeDataGlobal(tableId, compression, blockId, cProfiles, colIdSTypeMap, tStore, rStore, eStore, histograms);

  }

  @Override
  public long putDataJdbc(String tableName,
                          ResultSet resultSet) {
    return storeLocalService.putDataJdbc(tableName, resultSet);
  }

  private long putDataJdbcLocal(String tableName,
                                ResultSet resultSet) {
    byte tableId = metaModelApi.getTableId(tableName);
    boolean compression = metaModelApi.getTableCompression(tableName);

    List<CProfile> cProfiles = metaModelApi.getCProfiles(tableName);
    int colCount = cProfiles.size();

    /* Timestamp */
    TStore tStore = new TStore(1, cProfiles);

    // Fill histogram data
    Map<Integer, HEntry> histograms = new HashMap<>();
    cProfiles.stream()
        .filter(isNotTimestamp)
        .forEach(e -> histograms.put(e.getColId(), new HEntry(new ArrayList<>(), new ArrayList<>())));

    List<Integer> iColumn = IntStream.range(0, colCount).boxed().toList();

    try {
      final AtomicInteger iRow = new AtomicInteger(0);

      while (resultSet.next()) {
        int iR = iRow.getAndAdd(1);

        iColumn.forEach(iC -> {

          try {
            CProfile cProfile = cProfiles.get(iC);
            CSType csType = cProfile.getCsType();

            Object currObject = resultSet.getObject(cProfile.getColIdSql());

            // Fill timestamps
            if (csType.isTimeStamp()) {
              if (csType.getSType() == SType.RAW) {
                tStore.add(iC, iR, this.converter.getKeyValue(currObject, cProfile));
              }
            }

            if (!csType.isTimeStamp()) {
              // Fill histogram data
              int colId = cProfile.getColId();
              int prevValue = getPrevValue(histograms, colId);
              int currValue = this.converter.convertRawToInt(currObject, cProfile);

              if (prevValue != currValue) {
                histograms.get(colId).getIndex().add(iR);
                histograms.get(colId).getValue().add(currValue);
              } else if (iR == 0) {
                histograms.get(colId).getIndex().add(iR);
                histograms.get(colId).getValue().add(currValue);
              }
            }

          } catch (SQLException e) {
            log.catching(e);
            throw new RuntimeException(e);
          }
        });
      }

      if (tStore.size() == 0) {
        return -1;
      }

      long blockId = tStore.getBlockId();

      /* Store data and metadata */
      this.storeDataLocal(tableId, compression, blockId, cProfiles, tStore, histograms);

      return tStore.getTail();

    } catch (Exception e) {
      log.catching(e);
      throw new RuntimeException(e);
    }
  }

  private int getPrevValue(Map<Integer, HEntry> histograms,
                           int colId) {
    int size = histograms.get(colId).getValue().size();

    if (size == 0) {
      return Mapper.INT_NULL;
    }

    return histograms.get(colId).getValue().get(size - 1);
  }

  private void storeDataGlobal(byte tableId,
                               boolean compression,
                               long blockId,
                               List<CProfile> cProfiles,
                               Map<Integer, SType> colIdSTypeMap,
                               TStore tStore,
                               RStore rStore,
                               EStore eStore,
                               Map<Integer, HEntry> histograms) {

    /* Store metadata */
    this.storeMetadataLocal(tableId, blockId, cProfiles, colIdSTypeMap);

    /* Store raw data */
    this.storeRaw(tableId, compression, blockId, tStore, rStore);

    /* Store enum data */
    this.storeEnum(tableId, compression, blockId, eStore);

    /* Store histogram data */
    this.storeHistogramsEntry(tableId, compression, blockId, histograms);
  }

  private void storeRaw(byte tableId,
                        boolean compression,
                        long blockId,
                        TStore tStore,
                        RStore rStore) {
    if (compression) {
      rawDAO.putCompressed(tableId, blockId,
                           tStore.getMapping(), tStore.getRawData(),
                           rStore.getMappingInt(), rStore.getRawDataInt(),
                           rStore.getMappingLong(), rStore.getRawDataLong(),
                           rStore.getMappingFloat(), rStore.getRawDataFloat(),
                           rStore.getMappingDouble(), rStore.getRawDataDouble(),
                           rStore.getMappingString(), rStore.getRawDataString());

      return;
    }

    this.rawDAO.putLong(tableId, blockId, tStore.mappingToArray(), tStore.dataToArray());

    if (rStore.getInitialCapacityInt() > 0) {
      this.rawDAO.putInt(tableId, blockId,
                         rStore.getMappingInt().keySet().stream().mapToInt(i -> i)
                             .toArray(), getArrayInt(rStore.getRawDataInt()));
    }
    if (rStore.getInitialCapacityLong() > 0) {
      this.rawDAO.putLong(tableId, blockId,
                          rStore.getMappingLong().keySet().stream().mapToInt(i -> i)
                              .toArray(), getArrayLong(rStore.getRawDataLong()));
    }
    if (rStore.getInitialCapacityFloat() > 0) {
      this.rawDAO.putFloat(tableId, blockId,
                           rStore.getMappingFloat().keySet().stream().mapToInt(i -> i)
                               .toArray(), getArrayFloat(rStore.getRawDataFloat()));
    }
    if (rStore.getInitialCapacityDouble() > 0) {
      this.rawDAO.putDouble(tableId, blockId,
                            rStore.getMappingDouble().keySet().stream().mapToInt(i -> i)
                                .toArray(), getArrayDouble(rStore.getRawDataDouble()));
    }
    if (rStore.getInitialCapacityString() > 0) {
      this.rawDAO.putString(tableId, blockId,
                            rStore.getMappingString().keySet().stream().mapToInt(i -> i)
                                .toArray(), getArrayString(rStore.getRawDataString()));
    }
  }

  private void storeDataLocal(byte tableId,
                              boolean compression,
                              long blockId,
                              List<CProfile> cProfiles,
                              TStore tStore,
                              Map<Integer, HEntry> histograms) {

    Map<Integer, SType> colIdSTypeMap = new HashMap<>();

    histograms.forEach((colId, value) -> {
      int sizeOfRaw = tStore.size();
      int sizeOfHist = value.getValue().size();

      if (sizeOfHist < (sizeOfRaw / 2)) {
        colIdSTypeMap.put(colId, SType.HISTOGRAM);
      } else {
        if (value.getValue().stream().distinct().count() > 255) {
          colIdSTypeMap.put(colId, SType.RAW);
        } else {
          colIdSTypeMap.put(colId, SType.ENUM);
        }
      }
    });

    /* Raw */
    RStore rStore = new RStore(converter, cProfiles, colIdSTypeMap);

    /* Enums */
    EStore eStore = new EStore(cProfiles, colIdSTypeMap);

    cProfiles.forEach(cProfile -> {
      int colId = cProfile.getColId();

      if (SType.ENUM.equals(colIdSTypeMap.get(colId))) {
        int iMapping = eStore.getMapping().get(colId);

        List<Integer> indexList = histograms.get(colId).getIndex();
        List<Integer> valueList = histograms.get(colId).getValue();

        for (int i = 0; i < indexList.size(); i++) {
          int curIndex = indexList.get(i);
          int nextIndex;

          int curValue = valueList.get(i);

          if (indexList.size() != i + 1) {
            nextIndex = indexList.get(i + 1);

            for (int j = curIndex; j < nextIndex; j++) {
              try {
                eStore.add(iMapping, j, curValue);
              } catch (EnumByteExceedException e) {
                throw new RuntimeException(e);
              }
            }
          } else {
            nextIndex = tStore.size() - 1;

            for (int j = curIndex; j <= nextIndex; j++) {
              try {
                eStore.add(iMapping, j, curValue);
              } catch (EnumByteExceedException e) {
                throw new RuntimeException(e);
              }
            }
          }
        }
      } else if (SType.RAW.equals(colIdSTypeMap.get(cProfile.getColId()))) {
        List<Integer> indexList = histograms.get(colId).getIndex();
        List<Integer> valueList = histograms.get(colId).getValue();

        for (int i = 0; i < indexList.size(); i++) {
          int curIndex = indexList.get(i);
          int nextIndex;

          int curValue = valueList.get(i);

          if (indexList.size() != i + 1) {
            nextIndex = indexList.get(i + 1);

            for (int j = curIndex; j < nextIndex; j++) {
              rStore.add(Mapper.isCType(cProfile), cProfile, j, curValue);
            }
          } else {
            nextIndex = tStore.size() - 1;

            for (int j = curIndex; j <= nextIndex; j++) {
              rStore.add(Mapper.isCType(cProfile), cProfile, j, curValue);
            }
          }
        }
      }
    });

    /* Store metadata */
    this.storeMetadataLocal(tableId, blockId, cProfiles, colIdSTypeMap);

    /* Store raw data */
    this.storeRaw(tableId, compression, blockId, tStore, rStore);

    /* Store enum data */
    this.storeEnum(tableId, compression, blockId, eStore);

    /* Store histogram data */
    colIdSTypeMap.entrySet().stream()
        .filter(f -> !SType.HISTOGRAM.equals(f.getValue()))
        .forEach(obj -> histograms.remove(obj.getKey()));

    this.storeHistogramsEntry(tableId, compression, blockId, histograms);
  }

  @Override
  public void putDataJdbcBatch(String tableName,
                               ResultSet resultSet,
                               Integer fBaseBatchSize) {
    byte tableId = metaModelApi.getTableId(tableName);
    boolean compression = metaModelApi.getTableCompression(tableName);

    List<CProfile> cProfiles = metaModelApi.getCProfiles(tableName);
    int colCount = cProfiles.size();

    Map<Integer, SType> colIdSTypeMap = new HashMap<>();

    cProfiles.stream().filter(isNotTimestamp)
        .forEach(e -> colIdSTypeMap.put(e.getColId(), e.getCsType().getSType()));

    /* Timestamp */
    TStore tStore = new TStore(1, cProfiles);

    /* Raw */
    RStore rStore = new RStore(converter, cProfiles, colIdSTypeMap);

    /* Enum */
    EStore eStore = new EStore(cProfiles, colIdSTypeMap);

    /* Histogram */
    Map<Integer, HEntry> histograms = new HashMap<>();
    cProfiles.stream()
        .filter(isNotTimestamp)
        .forEach(e -> histograms.put(e.getColId(), new HEntry(new ArrayList<>(), new ArrayList<>())));

    try {
      final AtomicInteger iRow = new AtomicInteger(0);

      while (resultSet.next()) {
        int iR = iRow.getAndAdd(1);

        // Reinitialize
        if (iR == fBaseBatchSize) {
          long blockId = tStore.getBlockId();

          this.storeDataGlobal(tableId, compression, blockId, cProfiles, colIdSTypeMap, tStore, rStore, eStore, histograms);

          // Reset
          iRow.set(0);
          iR = iRow.getAndAdd(1);

          /* Timestamp */
          tStore = new TStore(1, cProfiles);

          /* Raw */
          rStore = new RStore(converter, cProfiles, colIdSTypeMap);

          /* Enum */
          eStore = new EStore(cProfiles, colIdSTypeMap);

          /* Histogram */
          histograms.clear();
          cProfiles.stream()
              .filter(isNotTimestamp)
              .forEach(e -> histograms.put(e.getColId(), new HEntry(new ArrayList<>(), new ArrayList<>())));
        }

        for (int iC = 0; iC < colCount; iC++) {

          try {
            CProfile cProfile = cProfiles.get(iC);
            int colId = cProfile.getColId();
            CSType csType = cProfile.getCsType();

            Object currObject = resultSet.getObject(cProfile.getColIdSql());

            // Fill timestamps
            if (csType.isTimeStamp()) {
              if (csType.getSType() == SType.RAW) {
                tStore.add(iC, iR, this.converter.getKeyValue(currObject, cProfile));
              }
            }

            // Fill raw data
            if (csType.getSType() == SType.RAW) {
              if (!csType.isTimeStamp()) {
                rStore.add(cProfile, iR, currObject);
              }
            }

            // Fill enum data
            if (csType.getSType() == SType.ENUM) {
              int curValue = this.converter.convertRawToInt(currObject, cProfile);
              int iMapping = eStore.getMapping().get(colId);
              try {
                eStore.add(iMapping, iR, curValue);
              } catch (EnumByteExceedException e) {
                throw new RuntimeException(e);
              }
            }

            // Fill histogram data
            if (csType.getSType() == SType.HISTOGRAM) {
              int prevValue = getPrevValue(histograms, colId);
              int currValue = this.converter.convertRawToInt(currObject, cProfile);

              if (prevValue != currValue) {
                histograms.get(colId).getIndex().add(iR);
                histograms.get(colId).getValue().add(currValue);
              } else if (iR == 0) {
                histograms.get(colId).getIndex().add(iR);
                histograms.get(colId).getValue().add(currValue);
              }
            }

          } catch (SQLException e) {
            log.catching(e);
            throw new RuntimeException(e);
          }
        }

      }

      if (iRow.get() <= fBaseBatchSize) {
        long blockId = tStore.getBlockId();

        this.storeDataGlobal(tableId, compression, blockId, cProfiles, colIdSTypeMap, tStore, rStore, eStore, histograms);

        log.info("Final flush for iRow: " + iRow.get());
      }

    } catch (Exception e) {
      log.catching(e);
      throw new RuntimeException(e);
    }

  }

  @Override
  public void putDataCsvBatch(String tableName,
                              String fileName,
                              String csvSplitBy,
                              Integer fBaseBatchSize) {
    byte tableId = metaModelApi.getTableId(tableName);
    boolean compression = metaModelApi.getTableCompression(tableName);
    List<CProfile> cProfiles = metaModelApi.getCProfiles(tableName);

    final AtomicLong counter = new AtomicLong(rawDAO.getLastBlockId(tableId));

    /* Long */
    int colRawDataLongCount = Mapper.getColumnCount(cProfiles, isRaw, isLong);
    List<List<Long>> rawDataLong = new ArrayList<>(colRawDataLongCount);
    fillArrayList(rawDataLong, colRawDataLongCount);
    List<Integer> rawDataLongMapping = new ArrayList<>(colRawDataLongCount);
    fillMappingRaw(cProfiles, rawDataLongMapping, isRaw, isLong);

    /* Double */
    int colRawDataDoubleCount = Mapper.getColumnCount(cProfiles, isRaw, isDouble);
    List<List<Double>> rawDataDouble = new ArrayList<>(colRawDataDoubleCount);
    fillArrayList(rawDataDouble, colRawDataDoubleCount);
    List<Integer> rawDataDoubleMapping = new ArrayList<>(colRawDataDoubleCount);
    fillMappingRaw(cProfiles, rawDataDoubleMapping, isRaw, isDouble);

    /* String */
    int colRawDataStringCount = Mapper.getColumnCount(cProfiles, isRaw, isString);
    List<List<String>> rawDataString = new ArrayList<>(colRawDataStringCount);
    fillArrayList(rawDataString, colRawDataStringCount);
    List<Integer> rawDataStringMapping = new ArrayList<>(colRawDataStringCount);
    fillMappingRaw(cProfiles, rawDataStringMapping, isRaw, isString);

    String line = "";
    try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
      line = br.readLine();
      String[] headers = line.split(csvSplitBy);
      log.info("Header = " + Arrays.toString(headers));

      final AtomicInteger iRow = new AtomicInteger(0);
      while ((line = br.readLine()) != null) {

        int iR = iRow.getAndAdd(1);

        // Reinitialize
        if (iR == fBaseBatchSize) {
          long blockId = counter.getAndAdd(1);

          /* Store metadata */
          this.storeMetadata(tableId, blockId, cProfiles);

          this.storeData(tableId, compression, blockId,
                         colRawDataLongCount, rawDataLongMapping, rawDataLong,
                         colRawDataDoubleCount, rawDataDoubleMapping, rawDataDouble,
                         colRawDataStringCount, rawDataStringMapping, rawDataString);

          iRow.set(0);
          iRow.getAndAdd(1);

          /* Long */
          rawDataLong = new ArrayList<>(colRawDataLongCount);
          fillArrayList(rawDataLong, colRawDataLongCount);
          rawDataLongMapping = new ArrayList<>(colRawDataLongCount);
          fillMappingRaw(cProfiles, rawDataLongMapping, isRaw, isLong);

          /* Double */
          rawDataDouble = new ArrayList<>(colRawDataDoubleCount);
          fillArrayList(rawDataDouble, colRawDataDoubleCount);
          rawDataDoubleMapping = new ArrayList<>(colRawDataDoubleCount);
          fillMappingRaw(cProfiles, rawDataDoubleMapping, isRaw, isDouble);

          /* String */
          rawDataString = new ArrayList<>(colRawDataStringCount);
          fillArrayList(rawDataString, colRawDataStringCount);
          rawDataStringMapping = new ArrayList<>(colRawDataStringCount);
          fillMappingRaw(cProfiles, rawDataStringMapping, isRaw, isString);
        }

        String[] data = line.split(csvSplitBy);

        for (int iC = 0; iC < headers.length; iC++) {
          String header = headers[iC];
          String colData = data[iC];

          Optional<CProfile> optionalCProfile = cProfiles.stream()
              .filter(f -> f.getColName().equals(header))
              .findAny();

          // Fill raw data
          if (optionalCProfile.isPresent()) {
            CProfile cProfile = optionalCProfile.get();
            if (cProfile.getCsType().getSType() == SType.RAW) {
              if (CType.LONG == Mapper.isCType(cProfile)) {
                rawDataLong.get(rawDataLongMapping.indexOf(iC)).add(Long.valueOf(colData));
              } else if (CType.DOUBLE == Mapper.isCType(cProfile)) {
                rawDataDouble.get(rawDataDoubleMapping.indexOf(iC)).add(Double.valueOf(colData));
              } else if (CType.STRING == Mapper.isCType(cProfile)) {
                rawDataString.get(rawDataStringMapping.indexOf(iC)).add(colData);
              }
            }
          }
        }
      }

      if (iRow.get() <= fBaseBatchSize) {
        long blockId = counter.getAndAdd(1);

        /* Store metadata */
        this.storeMetadata(tableId, blockId, cProfiles);

        this.storeData(tableId, compression, blockId,
                       colRawDataLongCount, rawDataLongMapping, rawDataLong,
                       colRawDataDoubleCount, rawDataDoubleMapping, rawDataDouble,
                       colRawDataStringCount, rawDataStringMapping, rawDataString);

        log.info("Final flush for iRow: " + iRow.get());
      }

    } catch (IOException e) {
      log.catching(e);
    }

  }

  private void storeMetadata(byte tableId,
                             long blockId,
                             List<CProfile> cProfiles) {
    List<Byte> rawCTypeKeys = new ArrayList<>();
    List<Integer> rawColIds = new ArrayList<>();
    List<Integer> enumColIds = new ArrayList<>();
    List<Integer> histogramColIds = new ArrayList<>();

    cProfiles.forEach(cProfile -> {
      if (SType.RAW.equals(cProfile.getCsType().getSType())) {
        rawCTypeKeys.add(cProfile.getCsType().getCType().getKey());
        rawColIds.add(cProfile.getColId());
      } else if (SType.ENUM.equals(cProfile.getCsType().getSType())) {
        enumColIds.add(cProfile.getColId());
      } else if (SType.HISTOGRAM.equals(cProfile.getCsType().getSType())) {
        histogramColIds.add(cProfile.getColId());
      }
    });

    this.rawDAO.putMetadata(tableId, blockId, getByteFromList(rawCTypeKeys),
                            rawColIds.stream().mapToInt(j -> j).toArray(),
                            enumColIds.stream().mapToInt(j -> j).toArray(),
                            histogramColIds.stream().mapToInt(j -> j).toArray());
  }

  private void storeMetadataLocal(byte tableId,
                                  long blockId,
                                  List<CProfile> cProfiles,
                                  Map<Integer, SType> colIdSTypeMap) {
    List<Byte> rawCTypeKeys = new ArrayList<>();
    List<Integer> rawColIds = new ArrayList<>();
    List<Integer> enumColIds = new ArrayList<>();
    List<Integer> histogramColIds = new ArrayList<>();

    cProfiles.forEach(cProfile -> {
      if (SType.RAW.equals(colIdSTypeMap.get(cProfile.getColId()))) {
        rawCTypeKeys.add(cProfile.getCsType().getCType().getKey());
        rawColIds.add(cProfile.getColId());
      } else if (SType.ENUM.equals(colIdSTypeMap.get(cProfile.getColId()))) {
        enumColIds.add(cProfile.getColId());
      } else if (SType.HISTOGRAM.equals(colIdSTypeMap.get(cProfile.getColId()))) {
        histogramColIds.add(cProfile.getColId());
      }
    });

    this.rawDAO.putMetadata(tableId, blockId, getByteFromList(rawCTypeKeys),
                            rawColIds.stream().mapToInt(j -> j).toArray(),
                            enumColIds.stream().mapToInt(j -> j).toArray(),
                            histogramColIds.stream().mapToInt(j -> j).toArray());
  }

  private void storeHistogramsEntry(byte tableID,
                                    boolean compression,
                                    long blockId,
                                    Map<Integer, HEntry> histograms) {

    histograms.forEach((colId, hEntry) -> {
      if (!hEntry.getIndex().isEmpty()) {
        if (compression) {
          this.histogramDAO.putCompressedKeysValues(tableID, blockId, colId,
                                                    hEntry.getIndex().stream().mapToInt(Integer::intValue).toArray(),
                                                    hEntry.getValue().stream().mapToInt(Integer::intValue).toArray());
        } else {
          this.histogramDAO.put(tableID, blockId, colId, getArrayFromMapHEntry(hEntry));
        }
      }
    });

  }

  private void storeEnum(byte tableId,
                         boolean compression,
                         long blockId,
                         EStore eStore) {

    if (eStore.getInitialCapacity() > 0) {
      eStore.getMapping().forEach((colId, iMapping) -> {

        int[] values = new int[eStore.getRawDataEColumn().get(iMapping).size()];

        AtomicInteger counter = new AtomicInteger(0);
        eStore.getRawDataEColumn().get(iMapping)
            .forEach((enumKey, enumValue) -> values[counter.getAndAdd(1)] = enumKey);

        try {
          this.enumDAO.putEColumn(tableId, blockId, colId, values,
                                  getByteFromList(eStore.getRawData().get(iMapping)), compression);
        } catch (IOException e) {
          log.catching(e);
          throw new RuntimeException(e);
        }
      });
    }

  }

  private void storeData(byte tableId,
                         boolean compression,
                         long blockId,
                         int colRawDataLongCount,
                         List<Integer> rawDataLongMapping,
                         List<List<Long>> rawDataLong,
                         int colRawDataDoubleCount,
                         List<Integer> rawDataDoubleMapping,
                         List<List<Double>> rawDataDouble,
                         int colRawDataStringCount,
                         List<Integer> rawDataStringMapping,
                         List<List<String>> rawDataString) {

    /* Store raw data entity */
    if (compression) {
      try {
        rawDAO.putCompressed(tableId, blockId,
                             Collections.emptyList(), Collections.emptyList(),
                             Collections.emptyList(), Collections.emptyList(),
                             rawDataLongMapping, rawDataLong,
                             Collections.emptyList(), Collections.emptyList(),
                             rawDataDoubleMapping, rawDataDouble,
                             rawDataStringMapping, rawDataString);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      return;
    }

    if (colRawDataLongCount > 0) {
      this.rawDAO.putLong(tableId, blockId, rawDataLongMapping.stream().mapToInt(i -> i)
          .toArray(), getArrayLong(rawDataLong));
    }
    if (colRawDataDoubleCount > 0) {
      this.rawDAO.putDouble(tableId, blockId, rawDataDoubleMapping.stream().mapToInt(i -> i)
          .toArray(), getArrayDouble(rawDataDouble));
    }
    if (colRawDataStringCount > 0) {
      this.rawDAO.putString(tableId, blockId, rawDataStringMapping.stream().mapToInt(i -> i)
          .toArray(), getArrayString(rawDataString));
    }
  }
}
