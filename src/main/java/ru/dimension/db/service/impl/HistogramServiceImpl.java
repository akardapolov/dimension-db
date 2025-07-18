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
import ru.dimension.db.service.HistogramService;
import ru.dimension.db.storage.Converter;
import ru.dimension.db.storage.HistogramDAO;
import ru.dimension.db.storage.RawDAO;

@Log4j2
public class HistogramServiceImpl extends CommonServiceApi implements HistogramService {
  private final MetaModelApi metaModelApi;
  private final Converter converter;
  private final HistogramDAO histogramDAO;
  private final RawDAO rawDAO;

  public HistogramServiceImpl(MetaModelApi metaModelApi,
                              Converter converter,
                              HistogramDAO histogramDAO,
                              RawDAO rawDAO) {
    this.metaModelApi = metaModelApi;
    this.converter = converter;
    this.histogramDAO = histogramDAO;
    this.rawDAO = rawDAO;
  }

  @Override
  public List<StackedColumn> getListStackedColumn(String tableName,
                                                  CProfile cProfile,
                                                  long begin,
                                                  long end)
      throws SqlColMetadataException {
    byte tableId = metaModelApi.getTableId(tableName);
    List<CProfile> cProfiles = metaModelApi.getCProfiles(tableName);

    CProfile tsProfile = getTimestampProfile(cProfiles);

    if (!tsProfile.getCsType().isTimeStamp()) {
      throw new SqlColMetadataException("Timestamp column not defined..");
    }

    if (cProfile.getCsType().isTimeStamp()) {
      throw new SqlColMetadataException("Not supported for timestamp column..");
    }

    List<StackedColumn> list = new ArrayList<>();

    long previousBlockId = this.rawDAO.getPreviousBlockId(tableId, begin);

    if (previousBlockId != begin & previousBlockId != 0) {
      long[] timestamps = rawDAO.getRawLong(tableId, previousBlockId, tsProfile.getColId());
      this.computeIndexedForStackedBeginEnd(tableId, cProfile, previousBlockId, timestamps, begin, end, list);
    }

    this.rawDAO.getListBlockIds(tableId, begin, end)
        .forEach(blockId -> {
          long[] timestamps = rawDAO.getRawLong(tableId, blockId, tsProfile.getColId());

          long tail = timestamps[timestamps.length - 1];

          if (tail > end) {
            this.computeIndexedForStackedBeginEnd(tableId, cProfile, blockId, timestamps, begin, end, list);
          } else {
            this.computeIndexedForStackedFull(tableId, cProfile, blockId, timestamps, list);
          }
        });

    return list;
  }

  private void computeIndexedForStackedFull(byte tableId,
                                            CProfile cProfile,
                                            long blockId,
                                            long[] timestamps,
                                            List<StackedColumn> list) {

    Map<Integer, Integer> map = new LinkedHashMap<>();

    long tail = timestamps[timestamps.length - 1];

    int[][] hData = histogramDAO.get(tableId, blockId, cProfile.getColId());

    IntStream iRow = IntStream.range(0, hData[0].length);
    iRow.forEach(iR -> {
      int deltaCountValue;

      if (iR == hData[0].length - 1) { //todo last row
        deltaCountValue = timestamps.length - hData[0][iR];
      } else {
        deltaCountValue = hData[0][iR + 1] - hData[0][iR];
      }

      map.compute(hData[1][iR], (k, val) -> val == null ? deltaCountValue : val + deltaCountValue);
    });

    Map<String, Integer> mapKeyCount = new LinkedHashMap<>();
    map.forEach((keyInt, value) -> mapKeyCount
        .put(this.converter.convertIntToRaw(keyInt, cProfile), value));

    if (!map.isEmpty()) {
      list.add(StackedColumn.builder()
                   .key(blockId)
                   .tail(tail)
                   .keyCount(mapKeyCount).build());
    }
  }

  private void computeIndexedForStackedBeginEnd(byte tableId,
                                                CProfile cProfile,
                                                long blockId,
                                                long[] timestamps,
                                                long begin,
                                                long end,
                                                List<StackedColumn> list) {

    long tail = timestamps[timestamps.length - 1];

    int[][] histograms = histogramDAO.get(tableId, blockId, cProfile.getColId());
    int[] histogramsUnPack = getHistogramUnPack(timestamps, histograms);

    Map<String, Integer> map = new LinkedHashMap<>();
    IntStream iRow = IntStream.range(0, timestamps.length);

    if (blockId < begin) {
      iRow.forEach(iR -> {
        if (timestamps[iR] >= begin & timestamps[iR] <= end) {
          String keyCompute = this.converter.convertIntToRaw(histogramsUnPack[iR], cProfile);
          map.compute(keyCompute, (k, val) -> val == null ? 1 : val + 1);
        }
      });
    }

    if (blockId >= begin & tail > end) {
      iRow.forEach(iR -> {
        if (timestamps[iR] >= begin & timestamps[iR] <= end) {
          String keyCompute = this.converter.convertIntToRaw(histogramsUnPack[iR], cProfile);
          map.compute(keyCompute, (k, val) -> val == null ? 1 : val + 1);
        }
      });
    }

    if (!map.isEmpty()) {
      list.add(StackedColumn.builder()
                   .key(blockId)
                   .tail(tail)
                   .keyCount(map).build());
    }
  }
}
