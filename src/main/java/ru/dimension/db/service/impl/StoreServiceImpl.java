package ru.dimension.db.service.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.extern.log4j.Log4j2;
import ru.dimension.db.core.metamodel.MetaModelApi;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.cstype.CSType;
import ru.dimension.db.model.profile.cstype.SType;
import ru.dimension.db.model.profile.table.AType;
import ru.dimension.db.model.profile.table.IType;
import ru.dimension.db.service.CommonServiceApi;
import ru.dimension.db.service.StatisticsService;
import ru.dimension.db.service.StoreService;
import ru.dimension.db.service.store.TStore;
import ru.dimension.db.service.store.UStore;
import ru.dimension.db.service.store.UStore.LFUCache;
import ru.dimension.db.storage.Converter;
import ru.dimension.db.storage.EnumDAO;
import ru.dimension.db.storage.HistogramDAO;
import ru.dimension.db.storage.RawDAO;
import ru.dimension.db.storage.format.StorageContext;
import ru.dimension.db.storage.format.StorageManager;

@Log4j2
public class StoreServiceImpl extends CommonServiceApi implements StoreService {

  private final MetaModelApi metaModelApi;

  private final Converter converter;

  private final EnumDAO enumDAO;

  private final RawDAO rawDAO;

  private final HistogramDAO histogramDAO;

  private final StatisticsService statisticsService;

  private final ConcurrentHashMap<Byte, ConcurrentHashMap<Integer, LFUCache>> globalConversionCache = new ConcurrentHashMap<>();

  // New unified storage manager
  private final StorageManager storageManager;

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

    // Initialize unified storage manager
    this.storageManager = new StorageManager();
  }

  @Override
  public void putDataDirect(String tableName,
                            List<List<Object>> data) {
    IType indexType = metaModelApi.getIndexType(tableName);
    if (IType.LOCAL.equals(indexType)) {
      log.info(IType.LOCAL);
      putDataDirectLocal(tableName, data);
    } else {
      putDataDirectGlobal(tableName, data);
    }
  }

  private void putDataDirectLocal(String tableName,
                                  List<List<Object>> data) {
    byte tableId = metaModelApi.getTableId(tableName);
    boolean compression = metaModelApi.getTableCompression(tableName);
    List<CProfile> cProfiles = metaModelApi.getCProfiles(tableName);
    int colCount = cProfiles.size();
    int rowCount = data.get(0).size();

    // Initialize storage type map
    Map<Integer, SType> colIdSTypeMap = new HashMap<>();
    cProfiles.stream()
        .filter(isNotTimestamp)
        .forEach(cProfile -> colIdSTypeMap.put(cProfile.getColId(),
                                               statisticsService.getLastSType(tableId,
                                                                              cProfile.getColId(),
                                                                              cProfile.getCsType().isTimeStamp())));

    // Get table cache from global cache
    ConcurrentHashMap<Integer, LFUCache> tableCache =
        globalConversionCache.computeIfAbsent(tableId, k -> new ConcurrentHashMap<>());

    // Initialize stores with cache
    TStore tStore = new TStore(1, cProfiles);
    UStore uStore = new UStore(converter, tableCache);

    // Initialize columns in UStore
    cProfiles.stream()
        .filter(isNotTimestamp)
        .forEach(cProfile -> uStore.addColumn(cProfile, colIdSTypeMap.get(cProfile.getColId())));

    Map<Integer, CProfile> colIdToProfile = new HashMap<>();
    cProfiles.forEach(p -> colIdToProfile.put(p.getColId(), p));

    try {
      boolean isStatByTableExist = statisticsService.isStatByTableExist(tableId);

      // Process data row by row
      for (int iR = 0; iR < rowCount; iR++) {
        for (int iC = 0; iC < colCount; iC++) {
          CProfile cProfile = cProfiles.get(iC);
          CSType csType = cProfile.getCsType();
          Object currObject = data.get(iC).get(iR);

          if (csType.isTimeStamp()) {
            tStore.add(iC, iR, converter.getKeyValue(currObject, cProfile));
          } else {
            uStore.add(cProfile, iR, currObject);
          }
        }
      }

      // Update SType statistic if table stats do not exist yet
      if (!isStatByTableExist) {
        uStore.analyzeAndConvertColumns(rowCount, colIdToProfile);
        colIdSTypeMap.clear();
        colIdSTypeMap.putAll(uStore.getStorageTypeMap());
      }

      if (tStore.size() == 0) return;
      long blockId = tStore.getBlockId();

      // Store data using unified StorageManager
      storeDataLocal(tableId, blockId, compression, cProfiles, tStore, uStore, colIdSTypeMap);

      // Full mode analysis after storing data
      AtomicInteger iRow = new AtomicInteger(rowCount);
      fullModeColumnAnalyze(tableId, cProfiles, uStore, iRow);

    } catch (Exception e) {
      log.catching(e);
      throw new RuntimeException(e);
    }
  }

  private void putDataDirectGlobal(String tableName,
                                   List<List<Object>> data) {
    byte tableId = metaModelApi.getTableId(tableName);
    boolean compression = metaModelApi.getTableCompression(tableName);
    List<CProfile> cProfiles = metaModelApi.getCProfiles(tableName);
    int colCount = cProfiles.size();
    int rowCount = data.getFirst().size();

    // Initialize storage type map
    Map<Integer, SType> colIdSTypeMap = new HashMap<>();
    cProfiles.stream()
        .filter(isNotTimestamp)
        .forEach(cProfile -> colIdSTypeMap.put(cProfile.getColId(), cProfile.getCsType().getSType()));

    // Get table cache from global cache
    ConcurrentHashMap<Integer, UStore.LFUCache> tableCache =
        globalConversionCache.computeIfAbsent(tableId, k -> new ConcurrentHashMap<>());

    // Initialize stores with cache
    TStore tStore = new TStore(1, cProfiles);
    UStore uStore = new UStore(converter, tableCache);

    // Initialize columns in UStore
    cProfiles.stream()
        .filter(isNotTimestamp)
        .forEach(cProfile -> uStore.addColumn(cProfile, colIdSTypeMap.get(cProfile.getColId())));

    try {
      // Process data row by row
      for (int iR = 0; iR < rowCount; iR++) {
        for (int iC = 0; iC < colCount; iC++) {
          CProfile cProfile = cProfiles.get(iC);
          CSType csType = cProfile.getCsType();
          Object currObject = data.get(iC).get(iR);

          if (csType.isTimeStamp()) {
            tStore.add(iC, iR, converter.getKeyValue(currObject, cProfile));
          } else {
            uStore.add(cProfile, iR, currObject);
          }
        }
      }

      if (tStore.size() == 0) return;
      long blockId = tStore.getBlockId();

      // Store data using unified StorageManager
      storeDataLocal(tableId, blockId, compression, cProfiles, tStore, uStore, colIdSTypeMap);

    } catch (Exception e) {
      log.catching(e);
      throw new RuntimeException(e);
    }
  }

  private long putDataJdbcLocal(String tableName,
                                ResultSet resultSet) {
    byte tableId = metaModelApi.getTableId(tableName);
    boolean compression = metaModelApi.getTableCompression(tableName);
    List<CProfile> cProfiles = metaModelApi.getCProfiles(tableName);
    int colCount = cProfiles.size();

    // Initialize storage type map
    Map<Integer, SType> colIdSTypeMap = new HashMap<>();
    cProfiles.stream()
        .filter(isNotTimestamp)
        .forEach(cProfile -> colIdSTypeMap.put(cProfile.getColId(),
                                               statisticsService.getLastSType(tableId,
                                                                              cProfile.getColId(),
                                                                              cProfile.getCsType().isTimeStamp())));

    // Get table cache from global cache
    ConcurrentHashMap<Integer, UStore.LFUCache> tableCache =
        globalConversionCache.computeIfAbsent(tableId, k -> new ConcurrentHashMap<>());

    // Initialize stores with cache
    TStore tStore = new TStore(1, cProfiles);
    UStore uStore = new UStore(converter, tableCache);

    // Initialize columns in UStore
    cProfiles.stream()
        .filter(isNotTimestamp)
        .forEach(cProfile -> uStore.addColumn(cProfile, colIdSTypeMap.get(cProfile.getColId())));

    Map<Integer, CProfile> colIdToProfile = new HashMap<>();
    cProfiles.forEach(p -> colIdToProfile.put(p.getColId(), p));

    try {
      boolean isStatByTableExist = statisticsService.isStatByTableExist(tableId);

      AtomicInteger iRow = new AtomicInteger(0);

      while (resultSet.next()) {
        int iR = iRow.getAndIncrement();

        for (int iC = 0; iC < colCount; iC++) {
          CProfile cProfile = cProfiles.get(iC);
          CSType csType = cProfile.getCsType();
          Object currObject = resultSet.getObject(cProfile.getColIdSql());

          if (csType.isTimeStamp()) {
            tStore.add(iC, iR, converter.getKeyValue(currObject, cProfile));
          } else {
            uStore.add(cProfile, iR, currObject);
          }
        }
      }

      // Update SType statistic
      if (!isStatByTableExist) {
        uStore.analyzeAndConvertColumns(iRow.get(), colIdToProfile);
        colIdSTypeMap.clear();
        colIdSTypeMap.putAll(uStore.getStorageTypeMap());
      }

      if (tStore.size() == 0) return -1;
      long blockId = tStore.getBlockId();

      // Store data using unified StorageManager
      storeDataLocal(tableId, blockId, compression, cProfiles, tStore, uStore, colIdSTypeMap);

      // Full mode analysis after storing data
      fullModeColumnAnalyze(tableId, cProfiles, uStore, iRow);

      return tStore.getTail();
    } catch (SQLException e) {
      log.catching(e);
      throw new RuntimeException(e);
    }
  }

  private void fullModeColumnAnalyze(byte tableId,
                                     List<CProfile> cProfiles,
                                     UStore uStore,
                                     AtomicInteger iRow) {
    String tableName = metaModelApi.getTableName(tableId);
    AType aType = metaModelApi.getAnalyzeType(tableName);

    if (aType == AType.ON_LOAD) {
      return;
    }

    List<CProfile> nonTimestampColumns = cProfiles.stream()
        .filter(isNotTimestamp)
        .collect(Collectors.toList());

    if (nonTimestampColumns.isEmpty()) {
      return;
    }

    if (!(statisticsService instanceof StatisticsServiceImpl)) {
      log.warn("Full mode analysis requires StatisticsServiceImpl");
      return;
    }

    StatisticsServiceImpl statisticsServiceImpl = (StatisticsServiceImpl) statisticsService;
    int rowCount = iRow.get();

    switch (aType) {
      case FULL_PASS_ONCE -> {
        if (statisticsServiceImpl.isFullPassDone(tableId)) {
          log.debug("Full pass already done for table {}", tableName);
          return;
        }
        log.info("Performing full pass analysis for table {}", tableName);
        for (CProfile cProfile : nonTimestampColumns) {
          int colId = cProfile.getColId();
          SType newSType = uStore.analyzeColumn(colId, rowCount, cProfile);
          statisticsServiceImpl.updateSType(tableId, colId, newSType);
        }
        statisticsServiceImpl.setFullPassDone(tableId);
      }
      case FULL_PASS_EACH -> {
        int colIdToAnalyze = getNextColumnToAnalyze(tableId, nonTimestampColumns, statisticsServiceImpl);
        CProfile cProfile = nonTimestampColumns.stream()
            .filter(c -> c.getColId() == colIdToAnalyze)
            .findFirst()
            .orElse(null);
        if (cProfile != null) {
          log.info("Analyzing column {} in table {}", cProfile.getColName(), tableName);
          SType newSType = uStore.analyzeColumn(colIdToAnalyze, rowCount, cProfile);
          statisticsServiceImpl.updateSType(tableId, colIdToAnalyze, newSType);
        }
      }
      default -> {
      }
    }
  }

  private int getNextColumnToAnalyze(byte tableId,
                                     List<CProfile> nonTimestampColumns,
                                     StatisticsServiceImpl statisticsService) {
    Integer lastColId = statisticsService.getLastAnalyzedColId(tableId);
    int index = 0;

    if (lastColId != null) {
      for (int i = 0; i < nonTimestampColumns.size(); i++) {
        if (nonTimestampColumns.get(i).getColId() == lastColId) {
          index = (i + 1) % nonTimestampColumns.size();
          break;
        }
      }
    }

    int nextColId = nonTimestampColumns.get(index).getColId();
    statisticsService.setLastAnalyzedColId(tableId, nextColId);
    return nextColId;
  }

  @Override
  public long putDataJdbc(String tableName, ResultSet resultSet) {
    IType indexType = metaModelApi.getIndexType(tableName);
    if (IType.LOCAL.equals(indexType)) {
      log.info(IType.LOCAL);
      return putDataJdbcLocal(tableName, resultSet);
    }

    byte tableId = metaModelApi.getTableId(tableName);
    boolean compression = metaModelApi.getTableCompression(tableName);
    List<CProfile> cProfiles = metaModelApi.getCProfiles(tableName);
    int colCount = cProfiles.size();

    // Initialize storage type map
    Map<Integer, SType> colIdSTypeMap = new HashMap<>();
    cProfiles.stream()
        .filter(isNotTimestamp)
        .forEach(cProfile -> colIdSTypeMap.put(cProfile.getColId(), cProfile.getCsType().getSType()));

    // Get table cache from global cache
    ConcurrentHashMap<Integer, UStore.LFUCache> tableCache =
        globalConversionCache.computeIfAbsent(tableId, k -> new ConcurrentHashMap<>());

    // Initialize stores with cache
    TStore tStore = new TStore(1, cProfiles);
    UStore uStore = new UStore(converter, tableCache);

    // Initialize columns in UStore
    cProfiles.stream()
        .filter(isNotTimestamp)
        .forEach(cProfile -> uStore.addColumn(cProfile, colIdSTypeMap.get(cProfile.getColId())));

    try {
      AtomicInteger iRow = new AtomicInteger(0);
      while (resultSet.next()) {
        int iR = iRow.getAndIncrement();

        for (int iC = 0; iC < colCount; iC++) {
          CProfile cProfile = cProfiles.get(iC);
          CSType csType = cProfile.getCsType();
          Object currObject = resultSet.getObject(cProfile.getColIdSql());

          if (csType.isTimeStamp()) {
            tStore.add(iC, iR, converter.getKeyValue(currObject, cProfile));
          } else {
            uStore.add(cProfile, iR, currObject);
          }
        }
      }

      if (tStore.size() == 0) return -1;
      long blockId = tStore.getBlockId();

      // Store data using unified StorageManager
      storeDataLocal(tableId, blockId, compression, cProfiles, tStore, uStore, colIdSTypeMap);
      return tStore.getTail();
    } catch (SQLException e) {
      log.catching(e);
      throw new RuntimeException(e);
    }
  }

  private void storeDataLocal(byte tableId,
                              long blockId,
                              boolean compression,
                              List<CProfile> cProfiles,
                              TStore tStore,
                              UStore uStore,
                              Map<Integer, SType> colIdSTypeMap) {

    // Create enhanced context for unified storage
    StorageContext context = StorageContext.builder()
        .tableId(tableId)
        .blockId(blockId)
        .timestamps(new long[0]) // Not needed for write operations
        .rawDAO(rawDAO)
        .enumDAO(enumDAO)
        .histogramDAO(histogramDAO)
        .converter(converter)
        .compressionEnabled(compression)
        .readRegistry(storageManager.getReadRegistry())
        .writeRegistry(storageManager.getWriteRegistry())
        .build();

    storeMetadataLocal(tableId, blockId, cProfiles, colIdSTypeMap);

    storeTStore(tableId, blockId, compression, tStore);

    Map<CProfile, SType> profileStorageMap = new HashMap<>();
    for (CProfile profile : cProfiles) {
      if (!profile.getCsType().isTimeStamp()) {
        SType sType = colIdSTypeMap.get(profile.getColId());
        profileStorageMap.put(profile, sType);
      }
    }

    storageManager.writeAllData(context, uStore, profileStorageMap);
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

  private void storeTStore(byte tableId,
                           long blockId,
                           boolean compression,
                           TStore tStore) {
    if (compression) {
      rawDAO.putCompressed(tableId, blockId, tStore.getMapping(), tStore.getRawData());
    } else {
      rawDAO.putLong(tableId, blockId, tStore.mappingToArray(), tStore.dataToArray());
    }
  }

  @Override
  public void putDataJdbcBatch(String tableName,
                               ResultSet resultSet,
                               Integer fBaseBatchSize) {
    IType indexType = metaModelApi.getIndexType(tableName);
    if (IType.LOCAL.equals(indexType)) {
      log.info(IType.LOCAL);
      putDataJdbcBatchLocal(tableName, resultSet, fBaseBatchSize);
    } else {
      putDataJdbcBatchGlobal(tableName, resultSet, fBaseBatchSize);
    }
  }

  private void putDataJdbcBatchLocal(String tableName,
                                     ResultSet resultSet,
                                     Integer fBaseBatchSize) {
    byte tableId = metaModelApi.getTableId(tableName);
    boolean compression = metaModelApi.getTableCompression(tableName);
    List<CProfile> cProfiles = metaModelApi.getCProfiles(tableName);
    int colCount = cProfiles.size();

    // Initialize storage type map once
    Map<Integer, SType> colIdSTypeMap = new HashMap<>();
    cProfiles.stream()
        .filter(isNotTimestamp)
        .forEach(cProfile -> colIdSTypeMap.put(cProfile.getColId(),
                                               statisticsService.getLastSType(tableId,
                                                                              cProfile.getColId(),
                                                                              cProfile.getCsType().isTimeStamp())));

    // Get table cache from global cache
    ConcurrentHashMap<Integer, UStore.LFUCache> tableCache =
        globalConversionCache.computeIfAbsent(tableId, k -> new ConcurrentHashMap<>());

    Map<Integer, CProfile> colIdToProfile = new HashMap<>();
    cProfiles.forEach(p -> colIdToProfile.put(p.getColId(), p));

    try {
      boolean isStatByTableExist = statisticsService.isStatByTableExist(tableId);
      AtomicInteger batchRowCounter = new AtomicInteger(0);
      AtomicInteger totalRowCounter = new AtomicInteger(0);

      // Use arrays to hold references (same approach as putDataJdbcBatchGlobal)
      TStore[] currentTStoreRef = {new TStore(fBaseBatchSize, cProfiles)};
      UStore[] currentUStoreRef = {new UStore(converter, tableCache)};

      // Initialize columns once
      cProfiles.stream()
          .filter(isNotTimestamp)
          .forEach(cProfile -> currentUStoreRef[0].addColumn(cProfile, colIdSTypeMap.get(cProfile.getColId())));

      while (resultSet.next()) {
        int batchRow = batchRowCounter.getAndIncrement();
        totalRowCounter.getAndIncrement();

        // Process current row
        for (int iC = 0; iC < colCount; iC++) {
          CProfile cProfile = cProfiles.get(iC);
          CSType csType = cProfile.getCsType();
          Object currObject = resultSet.getObject(cProfile.getColIdSql());

          if (csType.isTimeStamp()) {
            currentTStoreRef[0].add(iC, batchRow, converter.getKeyValue(currObject, cProfile));
          } else {
            currentUStoreRef[0].add(cProfile, batchRow, currObject);
          }
        }

        // Process batch immediately when size is reached
        if (batchRowCounter.get() >= fBaseBatchSize) {
          processBatchLocal(tableId, compression, cProfiles, currentTStoreRef[0], currentUStoreRef[0],
                            colIdSTypeMap, colIdToProfile, batchRowCounter.get(), isStatByTableExist);

          // Reset for next batch using array references
          batchRowCounter.set(0);
          currentTStoreRef[0] = new TStore(fBaseBatchSize, cProfiles);
          currentUStoreRef[0] = new UStore(converter, tableCache);

          // Reinitialize columns for new UStore
          cProfiles.stream()
              .filter(isNotTimestamp)
              .forEach(cProfile -> currentUStoreRef[0].addColumn(cProfile, colIdSTypeMap.get(cProfile.getColId())));

          // Update isStatByTableExist flag after first batch processing
          isStatByTableExist = true;

          log.debug("Processed batch, total rows: {}", String.format("%,d", totalRowCounter.get()));
        }
      }

      // Process final batch
      if (batchRowCounter.get() > 0) {
        processBatchLocal(tableId, compression, cProfiles, currentTStoreRef[0], currentUStoreRef[0],
                          colIdSTypeMap, colIdToProfile, batchRowCounter.get(), isStatByTableExist);
      }

      log.info("Total rows processed: {}", String.format("%,d", totalRowCounter.get()));

    } catch (SQLException e) {
      log.catching(e);
      throw new RuntimeException(e);
    }
  }

  private void putDataJdbcBatchGlobal(String tableName,
                                      ResultSet resultSet,
                                      Integer fBaseBatchSize) {
    byte tableId = metaModelApi.getTableId(tableName);
    boolean compression = metaModelApi.getTableCompression(tableName);
    List<CProfile> cProfiles = metaModelApi.getCProfiles(tableName);
    int colCount = cProfiles.size();

    Map<Integer, SType> colIdSTypeMap = new HashMap<>();
    cProfiles.stream()
        .filter(isNotTimestamp)
        .forEach(cProfile -> colIdSTypeMap.put(cProfile.getColId(),
                                               cProfile.getCsType().getSType()));

    ConcurrentHashMap<Integer, UStore.LFUCache> tableCache =
        globalConversionCache.computeIfAbsent(tableId, k -> new ConcurrentHashMap<>());

    try {
      AtomicInteger batchRowCounter = new AtomicInteger(0);
      AtomicInteger totalRowCounter = new AtomicInteger(0);

      // Use arrays to hold references
      TStore[] currentTStoreRef = {new TStore(1, cProfiles)};
      UStore[] currentUStoreRef = {new UStore(converter, tableCache)};

      // Initialize columns once
      cProfiles.stream()
          .filter(isNotTimestamp)
          .forEach(cProfile -> currentUStoreRef[0].addColumn(cProfile,
                                                             colIdSTypeMap.get(cProfile.getColId())));

      while (resultSet.next()) {
        int batchRow = batchRowCounter.getAndIncrement();
        totalRowCounter.getAndIncrement();

        for (int iC = 0; iC < colCount; iC++) {
          CProfile cProfile = cProfiles.get(iC);
          CSType csType = cProfile.getCsType();
          Object currObject = resultSet.getObject(cProfile.getColIdSql());

          if (csType.isTimeStamp()) {
            currentTStoreRef[0].add(iC, batchRow, converter.getKeyValue(currObject, cProfile));
          } else {
            currentUStoreRef[0].add(cProfile, batchRow, currObject);
          }
        }

        if (batchRowCounter.get() >= fBaseBatchSize) {
          processBatchGlobal(tableId, compression, cProfiles, currentTStoreRef[0], currentUStoreRef[0],
                             colIdSTypeMap, batchRowCounter.get());

          // Reset using array references
          batchRowCounter.set(0);
          currentTStoreRef[0] = new TStore(1, cProfiles);
          currentUStoreRef[0] = new UStore(converter, tableCache);

          cProfiles.stream()
              .filter(isNotTimestamp)
              .forEach(cProfile -> currentUStoreRef[0].addColumn(cProfile,
                                                                 colIdSTypeMap.get(cProfile.getColId())));

          log.debug("Processed batch, total rows: {}", String.format("%,d", totalRowCounter.get()));
        }
      }

      if (batchRowCounter.get() > 0) {
        processBatchGlobal(tableId, compression, cProfiles, currentTStoreRef[0], currentUStoreRef[0],
                           colIdSTypeMap, batchRowCounter.get());
      }

      log.info("Total rows processed: {}", String.format("%,d", totalRowCounter.get()));

    } catch (SQLException e) {
      log.catching(e);
      throw new RuntimeException(e);
    }
  }

  private void processBatchLocal(byte tableId, boolean compression, List<CProfile> cProfiles,
                                 TStore tStore, UStore uStore, Map<Integer, SType> colIdSTypeMap,
                                 Map<Integer, CProfile> colIdToProfile, int rowCount, boolean isStatByTableExist) {
    if (tStore.size() == 0) return;

    try {
      // Update SType statistic if needed
      if (!isStatByTableExist) {
        uStore.analyzeAndConvertColumns(rowCount, colIdToProfile);
        colIdSTypeMap.clear();
        colIdSTypeMap.putAll(uStore.getStorageTypeMap());
      }

      long blockId = tStore.getBlockId();

      storeDataLocal(tableId, blockId, compression, cProfiles, tStore, uStore, colIdSTypeMap);

      AtomicInteger iRow = new AtomicInteger(rowCount);
      fullModeColumnAnalyze(tableId, cProfiles, uStore, iRow);

      log.info("Processed batch with {} rows, blockId: {}", rowCount, blockId);

    } catch (Exception e) {
      log.catching(e);
      throw new RuntimeException(e);
    }
  }

  private void processBatchGlobal(byte tableId, boolean compression, List<CProfile> cProfiles,
                                  TStore tStore, UStore uStore, Map<Integer, SType> colIdSTypeMap,
                                  int rowCount) {
    if (tStore.size() == 0) return;

    try {
      long blockId = tStore.getBlockId();

      storeDataLocal(tableId, blockId, compression, cProfiles, tStore, uStore, colIdSTypeMap);

      log.info("Processed batch with {} rows, blockId: {}", rowCount, blockId);

    } catch (Exception e) {
      log.catching(e);
      throw new RuntimeException(e);
    }
  }
}