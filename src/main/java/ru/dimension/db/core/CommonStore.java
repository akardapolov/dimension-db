package ru.dimension.db.core;

import com.sleepycat.persist.EntityStore;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.log4j.Log4j2;
import ru.dimension.db.config.DBaseConfig;
import ru.dimension.db.config.FileConfig;
import ru.dimension.db.core.metamodel.MetaModelApi;
import ru.dimension.db.core.metamodel.MetaModelApiImpl;
import ru.dimension.db.exception.BeginEndWrongOrderException;
import ru.dimension.db.exception.EnumByteExceedException;
import ru.dimension.db.exception.GanttColumnNotSupportedException;
import ru.dimension.db.exception.SqlColMetadataException;
import ru.dimension.db.exception.TableNameEmptyException;
import ru.dimension.db.handler.MetaModelHandler;
import ru.dimension.db.handler.MetadataHandler;
import ru.dimension.db.model.GroupFunction;
import ru.dimension.db.model.MetaModel;
import ru.dimension.db.model.MetaModel.TableMetadata;
import ru.dimension.db.model.OrderBy;
import ru.dimension.db.model.filter.CompositeFilter;
import ru.dimension.db.model.output.BlockKeyTail;
import ru.dimension.db.model.output.GanttColumnCount;
import ru.dimension.db.model.output.GanttColumnSum;
import ru.dimension.db.model.output.StackedColumn;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.SProfile;
import ru.dimension.db.model.profile.TProfile;
import ru.dimension.db.model.profile.cstype.CSType;
import ru.dimension.db.model.profile.cstype.SType;
import ru.dimension.db.model.profile.table.BType;
import ru.dimension.db.model.profile.table.TType;
import ru.dimension.db.service.GroupByService;
import ru.dimension.db.service.RawService;
import ru.dimension.db.service.StatisticsService;
import ru.dimension.db.service.StoreService;
import ru.dimension.db.service.impl.StatisticsServiceImpl;
import ru.dimension.db.service.mapping.Mapper;
import ru.dimension.db.sql.BatchResultSet;
import ru.dimension.db.storage.Converter;
import ru.dimension.db.storage.DimensionDAO;
import ru.dimension.db.storage.EnumDAO;
import ru.dimension.db.storage.HistogramDAO;
import ru.dimension.db.storage.RawDAO;

@Log4j2
public abstract class CommonStore implements DStore {

  protected EntityStore store;

  protected MetaModel metaModel;
  protected MetaModelApi metaModelApi;
  protected FileConfig fileConfig;

  protected HistogramDAO histogramDAO;
  protected RawDAO rawDAO;
  protected DimensionDAO dimensionDAO;
  protected EnumDAO enumDAO;

  protected GroupByService groupByService;
  protected RawService rawService;
  protected StoreService storeService;

  protected Converter converter;
  protected StatisticsService statisticsService;

  public CommonStore(DBaseConfig dBaseConfig,
                     EntityStore store) {
    this.store = store;

    this.fileConfig = new FileConfig(dBaseConfig);

    try {
      this.metaModel = fileConfig.readObject() == null ? new MetaModel() : (MetaModel) fileConfig.readObject();
      this.metaModelApi = new MetaModelApiImpl(this.metaModel);
      this.statisticsService = new StatisticsServiceImpl();
    } catch (IOException | ClassNotFoundException e) {
      log.catching(e);
      throw new RuntimeException(e);
    }
  }

  public CommonStore(DBaseConfig dBaseConfig) {
    this.store = null;

    this.fileConfig = new FileConfig(dBaseConfig);

    try {
      this.metaModel = fileConfig.readObject() == null ? new MetaModel() : (MetaModel) fileConfig.readObject();
      this.metaModelApi = new MetaModelApiImpl(this.metaModel);
    } catch (IOException | ClassNotFoundException e) {
      log.catching(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public TProfile getTProfile(String tableName) throws TableNameEmptyException {
    if (Objects.isNull(tableName) || tableName.isBlank()) {
      throw new TableNameEmptyException("Empty table name. Please, define it explicitly..");
    }

    TProfile tProfile = new TProfile().setTableName(tableName);

    checkTableInMetamodel(tableName);

    List<CProfile> cProfiles = metaModel.getMetadata().get(tableName).getCProfiles();

    cProfiles.stream()
        .filter(cProfile -> cProfile.getCsType().isTimeStamp())
        .findAny()
        .ifPresentOrElse((value) -> {
                           tProfile.setTableType(TType.TIME_SERIES);
                           tProfile.setCProfiles(cProfiles);
                         },
                         () -> {
                           tProfile.setTableType(TType.REGULAR);
                           tProfile.setCProfiles(cProfiles);
                         });

    if (Objects.nonNull(metaModel.getMetadata().get(tableName))) {
      MetaModel.TableMetadata tableMetadata = metaModel.getMetadata().get(tableName);
      tProfile.setIndexType(tableMetadata.getIndexType());
      tProfile.setBackendType(tableMetadata.getBackendType());
      tProfile.setCompression(tableMetadata.getCompression());
    }

    return tProfile;
  }

  @Override
  public TProfile loadDirectTableMetadata(SProfile sProfile) throws TableNameEmptyException {
    checkAndInitializeMetaModel();
    return loadTableMetadata(sProfile,
                             () -> fetchMetadataDirect(sProfile),
                             () -> updateTimestampMetadata(sProfile.getTableName(), sProfile));
  }

  @Override
  public TProfile loadJdbcTableMetadata(Connection connection,
                                        String select,
                                        SProfile sProfile)
      throws TableNameEmptyException {
    checkAndInitializeMetaModel();
    return loadTableMetadata(sProfile,
                             () -> fetchMetadataFromJdbc(connection, select, sProfile),
                             () -> updateTimestampMetadata(sProfile.getTableName(), sProfile));
  }

  @Override
  public TProfile loadJdbcTableMetadata(Connection connection,
                                        String sqlSchemaName,
                                        String sqlTableName,
                                        SProfile sProfile)
      throws TableNameEmptyException {
    checkAndInitializeMetaModel();
    return loadTableMetadata(sProfile,
                             () -> fetchMetadataFromJdbc(connection, sqlSchemaName, sqlTableName, sProfile),
                             () -> updateTimestampMetadata(sProfile.getTableName(), sProfile));
  }

  @Override
  public void setTimestampColumn(String tableName,
                                 String timestampColumnName)
      throws TableNameEmptyException {

    checkTableInMetamodel(tableName);

    if (timestampColumnName.isBlank()) {
      log.warn("Empty timestamp column name");
      return;
    }

    Optional<CProfile> optionalTsCProfile = metaModel.getMetadata().get(tableName)
        .getCProfiles()
        .stream()
        .filter(cProfile -> cProfile.getCsType().isTimeStamp())
        .findAny();

    log.info("Set timestamp column: " + timestampColumnName + " for table: " + tableName);

    if (optionalTsCProfile.isPresent()) {
      for (CProfile cProfile : metaModel.getMetadata().get(tableName).getCProfiles()) {
        if (cProfile != null && optionalTsCProfile.get().getColName().equals(cProfile.getColName())) {
          cProfile.getCsType().setTimeStamp(false);
        }

        if (cProfile != null && timestampColumnName.equals(cProfile.getColName())) {
          cProfile.getCsType().setTimeStamp(true);
        }
      }
    } else {
      for (CProfile cProfile : metaModel.getMetadata().get(tableName).getCProfiles()) {
        if (cProfile != null && timestampColumnName.equals(cProfile.getColName())) {
          cProfile.getCsType().setTimeStamp(true);
          break;
        }
      }
    }
  }

  private void checkAndInitializeMetaModel() {
    if (metaModel.getMetadata().isEmpty()) {
      saveMetaModel();
    }
  }

  private TProfile loadTableMetadata(SProfile sProfile,
                                     Runnable fetchMetadata,
                                     Runnable updateTimestampMetadata)
      throws TableNameEmptyException {
    checkIsTableNameEmpty(sProfile);

    String tableName = sProfile.getTableName();
    TProfile tProfile = new TProfile().setTableName(tableName);

    if (metaModelExistsForTable(tableName)) {
      if (metaModelColumModelNotTheSame(tableName, sProfile)) {
        fetchMetadata.run();
      }
      updateTimestampMetadata.run();
      fillTProfileFromMetaModel(tableName, tProfile);
    } else {
      fetchMetadata.run();
      fillTProfileAndSaveMetaModel(tableName, sProfile, tProfile);
    }

    saveMetaModel();

    return tProfile;
  }

  private boolean metaModelExistsForTable(String tableName) {
    TableMetadata tableMetadata = metaModel.getMetadata().get(tableName);
    return tableMetadata != null && tableMetadata.getTableId() != null;
  }

  private boolean metaModelColumModelNotTheSame(String tableName,
                                                SProfile sProfile) {
    TableMetadata tableMetadata = metaModel.getMetadata().get(tableName);
    return tableMetadata.getCProfiles().size() != sProfile.getCsTypeMap().size();
  }

  private void fetchMetadataDirect(SProfile sProfile) {
    byte tableId = MetaModelHandler.getNextInternalTableId(metaModel);
    String tableName = sProfile.getTableName();

    if (sProfile.getCsTypeMap().isEmpty()) {
      throw new RuntimeException("Storage profile is empty");
    }

    try {
      List<CProfile> cProfileList = MetadataHandler.getDirectCProfileList(sProfile);

      metaModel.getMetadata().put(tableName, new TableMetadata()
          .setTableId(tableId)
          .setTableType(sProfile.getTableType())
          .setIndexType(sProfile.getIndexType())
          .setBackendType(sProfile.getBackendType())
          .setCompression(sProfile.getCompression())
          .setCProfiles(cProfileList));

      if (!BType.BERKLEYDB.equals(sProfile.getBackendType())) {
        putMetadataNonBdb(sProfile, tableId, cProfileList);
      }

    } catch (Exception e) {
      log.catching(e);
    }
  }

  private void putMetadataNonBdb(SProfile sProfile,
                                 byte tableId,
                                 List<CProfile> cProfileList) {
    try {
      List<Byte> rawCTypeKeys = cProfileList.stream()
          .filter(cProfile -> SType.RAW.equals(cProfile.getCsType().getSType()))
          .map(cProfile -> cProfile.getCsType().getCType().getKey())
          .collect(Collectors.toList());

      int[] rawColIds = cProfileList.stream()
          .filter(cProfile -> SType.RAW.equals(cProfile.getCsType().getSType()))
          .mapToInt(CProfile::getColId)
          .toArray();

      if (cProfileList.size() != rawCTypeKeys.size()) {
        throw new RuntimeException("Not supported for backend: " + sProfile.getBackendType());
      }

      this.rawDAO.putMetadata(tableId, 0L, getByteFromList(rawCTypeKeys), rawColIds, new int[0], new int[0]);

    } catch (Exception e) {
      log.catching(e);
    }
  }

  public byte[] getByteFromList(List<Byte> list) {
    byte[] byteArray = new byte[list.size()];
    int index = 0;
    for (byte b : list) {
      byteArray[index++] = b;
    }
    return byteArray;
  }

  private void fetchMetadataFromJdbc(Connection connection,
                                     String sqlSchemaName,
                                     String sqlTableName,
                                     SProfile sProfile) {
    if (!BType.ORACLE.equals(sProfile.getBackendType())) {
      throw new RuntimeException("Not supported backend type: " + sProfile.getBackendType());
    }
    byte tableId = MetaModelHandler.getNextInternalTableId(metaModel);
    String tableName = sProfile.getTableName();

    try {
      List<CProfile> cProfileList = MetadataHandler.getJdbcCProfileList(connection, sqlSchemaName, sqlTableName);

      Map<String, CSType> csTypeMap = sProfile.getCsTypeMap();

      cProfileList.forEach(cProfile -> {
        CSType csType = csTypeMap.getOrDefault(cProfile.getColName(), defaultCSType());
        csType.setCType(Mapper.isCType(cProfile));
        csType.setDType(Mapper.isDBType(cProfile));
        cProfile.setCsType(csType);
        log.info(cProfile);
      });

      metaModel.getMetadata().put(tableName, new TableMetadata()
          .setTableId(tableId)
          .setTableType(sProfile.getTableType())
          .setIndexType(sProfile.getIndexType())
          .setBackendType(sProfile.getBackendType())
          .setCompression(sProfile.getCompression())
          .setCProfiles(cProfileList));

      if (!BType.BERKLEYDB.equals(sProfile.getBackendType())) {
        putMetadataNonBdb(sProfile, tableId, cProfileList);
      }
    } catch (Exception e) {
      log.catching(e);
    }
  }

  private void fetchMetadataFromJdbc(Connection connection,
                                     String select,
                                     SProfile sProfile) {
    byte tableId = MetaModelHandler.getNextInternalTableId(metaModel);
    String tableName = sProfile.getTableName();

    try {
      List<CProfile> cProfileList = MetadataHandler.getJdbcCProfileList(connection, select);

      Map<String, CSType> csTypeMap = sProfile.getCsTypeMap();

      cProfileList.forEach(cProfile -> {
        CSType csType = csTypeMap.getOrDefault(cProfile.getColName(), defaultCSType());
        csType.setCType(Mapper.isCType(cProfile));
        csType.setDType(Mapper.isDBType(cProfile));
        cProfile.setCsType(csType);
        log.info(cProfile);
      });

      metaModel.getMetadata().put(tableName, new TableMetadata()
          .setTableId(tableId)
          .setTableType(sProfile.getTableType())
          .setIndexType(sProfile.getIndexType())
          .setBackendType(sProfile.getBackendType())
          .setCompression(sProfile.getCompression())
          .setCProfiles(cProfileList));

      if (!BType.BERKLEYDB.equals(sProfile.getBackendType())) {
        putMetadataNonBdb(sProfile, tableId, cProfileList);
      }
    } catch (Exception e) {
      log.catching(e);
    }
  }

  private CSType defaultCSType() {
    return new CSType().toBuilder()
        .isTimeStamp(false)
        .sType(SType.RAW)
        .build();
  }

  private void fillTProfileAndSaveMetaModel(String tableName,
                                            SProfile sProfile,
                                            TProfile tProfile) {
    tProfile.setTableType(sProfile.getTableType());
    tProfile.setCompression(sProfile.getCompression());

    TableMetadata tableMetadata = metaModel.getMetadata().get(tableName);

    if (tableMetadata != null) {
      List<CProfile> cProfiles = tableMetadata.getCProfiles();
      tProfile.setCProfiles(cProfiles);
    }

    saveMetaModel();
  }

  private void fillTProfileFromMetaModel(String tableName,
                                         TProfile tProfile) {
    TableMetadata tableMetadata = metaModel.getMetadata().get(tableName);
    if (tableMetadata != null) {
      List<CProfile> cProfileList = tableMetadata.getCProfiles();

      tProfile.setTableType(tableMetadata.getTableType());
      tProfile.setIndexType(tableMetadata.getIndexType());
      tProfile.setBackendType(tableMetadata.getBackendType());
      tProfile.setCompression(tableMetadata.getCompression());
      tProfile.setCProfiles(cProfileList);
    } else {
      log.warn("No metadata found for table: " + tableName);
    }
  }

  private void checkIsTableNameEmpty(SProfile sProfile) throws TableNameEmptyException {
    if (Objects.isNull(sProfile.getTableName()) || sProfile.getTableName().isBlank()) {
      throw new TableNameEmptyException("Empty table name. Please, define it explicitly..");
    }
  }

  private void updateTimestampMetadata(String tableName,
                                       SProfile sProfile) {
    Optional<CProfile> optionalTsCProfile = metaModel.getMetadata().get(tableName)
        .getCProfiles()
        .stream()
        .filter(cProfile -> cProfile.getCsType().isTimeStamp())
        .findAny();

    Optional<Entry<String, CSType>> optionalTsEntry = sProfile.getCsTypeMap().entrySet()
        .stream()
        .filter(entry -> Objects.nonNull(entry.getValue()))
        .filter(entry -> entry.getValue().isTimeStamp())
        .findAny();

    if (optionalTsCProfile.isEmpty() & optionalTsEntry.isPresent()) {
      log.info("Update timestamp column in DBase metadata");
      for (CProfile cProfile : metaModel.getMetadata().get(tableName).getCProfiles()) {
        if (cProfile != null && optionalTsEntry.get().getKey().equalsIgnoreCase(cProfile.getColName())) {
          cProfile.getCsType().setTimeStamp(true);
          break;
        }
      }
    }

    metaModel.getMetadata().get(tableName).setTableType(sProfile.getTableType());
    metaModel.getMetadata().get(tableName).setIndexType(sProfile.getIndexType());
    metaModel.getMetadata().get(tableName).setBackendType(sProfile.getBackendType());
    metaModel.getMetadata().get(tableName).setCompression(sProfile.getCompression());

    if (optionalTsCProfile.isEmpty() & optionalTsEntry.isEmpty()
        & !TType.TIME_SERIES.equals(sProfile.getTableType())) {
      log.warn("Timestamp column not defined");
    }
  }

  private void saveMetaModel() {
    try {
      fileConfig.saveObject(metaModel);
    } catch (IOException e) {
      log.catching(e);
      throw new RuntimeException(e);
    }
  }

  private void checkTableInMetamodel(String tableName) throws TableNameEmptyException {
    if (Objects.isNull(metaModel.getMetadata().get(tableName))) {
      log.warn("Metamodel for table name: " + tableName + " not found");
    }

    if (Objects.isNull(metaModel.getMetadata().get(tableName).getCProfiles())) {
      throw new TableNameEmptyException("Metamodel for table name: " + tableName + " not found");
    }
  }

  @Override
  public void putDataDirect(String tableName,
                            List<List<Object>> data) throws SqlColMetadataException, EnumByteExceedException {

    if (this.metaModel.getMetadata().get(tableName) == null) {
      throw new SqlColMetadataException("Empty sql column metadata for DBase instance..");
    }

    this.storeService.putDataDirect(tableName, data);
  }

  @Override
  public long putDataJdbc(String tableName,
                          ResultSet resultSet) throws SqlColMetadataException, EnumByteExceedException {

    if (this.metaModel.getMetadata().get(tableName) == null) {
      throw new SqlColMetadataException("Empty sql column metadata for DBase instance..");
    }

    return this.storeService.putDataJdbc(tableName, resultSet);
  }

  @Override
  public void putDataJdbcBatch(String tableName,
                               ResultSet resultSet,
                               Integer batchSize) throws SqlColMetadataException, EnumByteExceedException {

    if (this.metaModel.getMetadata().get(tableName) == null) {
      throw new SqlColMetadataException("Empty sql column metadata for DBase instance..");
    }

    this.storeService.putDataJdbcBatch(tableName, resultSet, batchSize);
  }

  @Override
  public List<BlockKeyTail> getBlockKeyTailList(String tableName,
                                                long begin,
                                                long end) throws BeginEndWrongOrderException, SqlColMetadataException {
    if (begin > end) {
      throw new BeginEndWrongOrderException("Begin value must be less the end one..");
    }

    return rawService.getBlockKeyTailList(tableName, begin, end);
  }

  @Override
  public List<StackedColumn> getStacked(String tableName,
                                        CProfile cProfile,
                                        GroupFunction groupFunction,
                                        CompositeFilter compositeFilter,
                                        long begin,
                                        long end)
      throws SqlColMetadataException, BeginEndWrongOrderException {
    if (begin > end) {
      throw new BeginEndWrongOrderException("Begin value must be less the end one..");
    }

    return this.groupByService.getStacked(tableName, cProfile, groupFunction, compositeFilter, begin, end);
  }

  @Override
  public List<GanttColumnCount> getGanttCount(String tableName,
                                              CProfile firstGrpBy,
                                              CProfile secondGrpBy,
                                              CompositeFilter compositeFilter,
                                              long begin,
                                              long end)
      throws BeginEndWrongOrderException, SqlColMetadataException {

    if (firstGrpBy.getCsType().isTimeStamp() | secondGrpBy.getCsType().isTimeStamp()) {
      throw new SqlColMetadataException("Group by not supported for timestamp column..");
    }

    if (begin > end) {
      throw new BeginEndWrongOrderException("Begin value must be less the end one..");
    }

    log.info("First column profile: " + firstGrpBy);
    log.info("Second column profile: " + secondGrpBy);

    return this.groupByService.getGanttCount(tableName, firstGrpBy, secondGrpBy, compositeFilter, begin, end);
  }

  @Override
  public List<GanttColumnCount> getGanttCount(String tableName,
                                              CProfile firstGrpBy,
                                              CProfile secondGrpBy,
                                              CompositeFilter compositeFilter,
                                              int batchSize,
                                              long begin,
                                              long end)
      throws BeginEndWrongOrderException, SqlColMetadataException {

    return groupByService.getGanttCount(tableName, firstGrpBy, secondGrpBy, compositeFilter, batchSize, begin, end);
  }

  @Override
  public List<GanttColumnSum> getGanttSum(String tableName,
                                          CProfile firstGrpBy,
                                          CProfile secondGrpBy,
                                          CompositeFilter compositeFilter,
                                          long begin,
                                          long end)
      throws SqlColMetadataException, BeginEndWrongOrderException, GanttColumnNotSupportedException {
    if (firstGrpBy.getCsType().isTimeStamp() | secondGrpBy.getCsType().isTimeStamp()) {
      throw new SqlColMetadataException("Group by not supported for timestamp column..");
    }

    if (begin > end) {
      throw new BeginEndWrongOrderException("Begin value must be less the end one..");
    }

    log.info("First column profile: " + firstGrpBy);
    log.info("Second column profile: " + secondGrpBy);

    return this.groupByService.getGanttSum(tableName, firstGrpBy, secondGrpBy, compositeFilter, begin, end);
  }

  @Override
  public List<String> getDistinct(String tableName,
                                  CProfile cProfile,
                                  OrderBy orderBy,
                                  CompositeFilter compositeFilter,
                                  int limit,
                                  long begin,
                                  long end)
      throws BeginEndWrongOrderException {
    if (begin > end) {
      throw new BeginEndWrongOrderException("Begin value must be less the end one..");
    }

    return groupByService.getDistinct(tableName, cProfile, orderBy, compositeFilter, limit, begin, end);
  }

  @Override
  public List<List<Object>> getRawDataByColumn(String tableName,
                                               CProfile cProfile,
                                               long begin,
                                               long end) {
    return rawService.getRawDataByColumn(tableName, cProfile, begin, end);
  }

  @Override
  public List<List<Object>> getRawDataAll(String tableName,
                                          long begin,
                                          long end) {
    return rawService.getRawDataAll(tableName, begin, end);
  }

  @Override
  public List<List<Object>> getRawDataAll(String tableName,
                                          CProfile cProfileFilter,
                                          String filter,
                                          long begin,
                                          long end) {
    return rawService.getRawDataAll(tableName, cProfileFilter, filter, begin, end);
  }

  @Override
  public BatchResultSet getBatchResultSet(String tableName,
                                          int fetchSize) {
    if (fetchSize <= 0) {
      log.warn("Fetch size can not be less or equal 0. Set to the default value of 1");
      fetchSize = 1;
    }

    return rawService.getBatchResultSet(tableName, 0L, Long.MAX_VALUE, fetchSize);
  }

  @Override
  public BatchResultSet getBatchResultSet(String tableName,
                                          long begin,
                                          long end,
                                          int fetchSize) {
    if (fetchSize <= 0) {
      log.warn("Fetch size can not be less or equal 0. Set to the default value of 1");
      fetchSize = 1;
    }

    return rawService.getBatchResultSet(tableName, begin, end, fetchSize);
  }

  @Override
  public long getFirst(String tableName,
                       long begin,
                       long end) {
    return rawService.getFirst(tableName, begin, end);
  }

  @Override
  public long getLast(String tableName,
                      long begin,
                      long end) {
    return rawService.getLast(tableName, begin, end);
  }

  @Override
  public void syncBackendDb() {
  }

  @Override
  public void closeBackendDb() {
  }
}
