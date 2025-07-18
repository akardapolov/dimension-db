package ru.dimension.db.service.impl;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import lombok.extern.log4j.Log4j2;
import ru.dimension.db.core.metamodel.MetaModelApi;
import ru.dimension.db.exception.SqlColMetadataException;
import ru.dimension.db.model.output.StackedColumn;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.service.CommonServiceApi;
import ru.dimension.db.service.EnumService;
import ru.dimension.db.storage.Converter;
import ru.dimension.db.storage.EnumDAO;
import ru.dimension.db.storage.RawDAO;
import ru.dimension.db.storage.bdb.entity.column.EColumn;
import ru.dimension.db.storage.helper.EnumHelper;

@Log4j2
public class EnumServiceImpl extends CommonServiceApi implements EnumService {
  private final MetaModelApi metaModelApi;
  private final Converter converter;
  private final RawDAO rawDAO;
  private final EnumDAO enumDAO;

  public EnumServiceImpl(MetaModelApi metaModelApi,
                         Converter converter,
                         RawDAO rawDAO,
                         EnumDAO enumDAO) {
    this.metaModelApi = metaModelApi;
    this.converter = converter;
    this.rawDAO = rawDAO;
    this.enumDAO = enumDAO;
  }

  @Override
  public List<StackedColumn> getListStackedColumn(String tableName,
                                                  CProfile cProfile,
                                                  long begin,
                                                  long end)
      throws SqlColMetadataException {
    byte tableId = metaModelApi.getTableId(tableName);
    List<CProfile> cProfiles = metaModelApi.getCProfiles(tableName);

    if (!getTimestampProfile(cProfiles).getCsType().isTimeStamp()) {
      throw new SqlColMetadataException("Timestamp column not defined..");
    }

    if (cProfile.getCsType().isTimeStamp()) {
      throw new SqlColMetadataException("Not supported for timestamp column..");
    }

    List<StackedColumn> list = new ArrayList<>();

    long previousBlockId = this.rawDAO.getPreviousBlockId(tableId, begin);

    if (previousBlockId != begin & previousBlockId != 0) {
      this.computeNoIndexBeginEnd(tableName, cProfile, previousBlockId, begin, end, list);
    }

    for (Long blockId : this.rawDAO.getListBlockIds(tableId, begin, end)) {
      this.computeNoIndexBeginEnd(tableName, cProfile, blockId, begin, end, list);
    }

    return list;
  }

  private void computeNoIndexBeginEnd(String tableName,
                                      CProfile cProfile,
                                      long blockId,
                                      long begin,
                                      long end,
                                      List<StackedColumn> list) {
    byte tableId = metaModelApi.getTableId(tableName);
    List<CProfile> cProfiles = metaModelApi.getCProfiles(tableId);

    CProfile tsProfile = getTimestampProfile(cProfiles);

    Map<Byte, Integer> map = new LinkedHashMap<>();

    long[] timestamps = this.rawDAO.getRawLong(tableId, blockId, tsProfile.getColId());

    EColumn eColumn = enumDAO.getEColumnValues(tableId, blockId, cProfile.getColId());

    long tail = timestamps[timestamps.length - 1];

    IntStream iRow = IntStream.range(0, timestamps.length);
    iRow.forEach(iR -> {
      if (timestamps[iR] >= begin & timestamps[iR] <= end) {
        map.compute(eColumn.getDataByte()[iR], (k, val) -> val == null ? 1 : val + 1);
      }
    });

    Map<String, Integer> mapKeyCount = new LinkedHashMap<>();

    map.forEach((keyByte, value) -> mapKeyCount.put(converter.convertIntToRaw(
        EnumHelper.getIndexValue(eColumn.getValues(), keyByte), cProfile), value));

    if (!map.isEmpty()) {
      list.add(StackedColumn.builder()
                   .key(blockId)
                   .tail(tail)
                   .keyCount(mapKeyCount).build());
    }
  }
}