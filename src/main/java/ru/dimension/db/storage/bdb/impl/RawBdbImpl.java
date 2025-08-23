package ru.dimension.db.storage.bdb.impl;

import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import lombok.extern.log4j.Log4j2;
import org.xerial.snappy.Snappy;
import ru.dimension.db.metadata.CompressType;
import ru.dimension.db.model.GroupFunction;
import ru.dimension.db.model.OrderBy;
import ru.dimension.db.model.filter.CompositeFilter;
import ru.dimension.db.model.output.GanttColumnCount;
import ru.dimension.db.model.output.GanttColumnSum;
import ru.dimension.db.model.output.StackedColumn;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.cstype.CType;
import ru.dimension.db.sql.BatchResultSet;
import ru.dimension.db.storage.RawDAO;
import ru.dimension.db.storage.bdb.QueryBdbApi;
import ru.dimension.db.storage.bdb.entity.ColumnKey;
import ru.dimension.db.storage.bdb.entity.Metadata;
import ru.dimension.db.storage.bdb.entity.MetadataKey;
import ru.dimension.db.storage.bdb.entity.column.RColumn;
import ru.dimension.db.util.CachedLastLinkedHashMap;

@Log4j2
public class RawBdbImpl extends QueryBdbApi implements RawDAO {

  private final PrimaryIndex<MetadataKey, Metadata> primaryIndex;
  private final PrimaryIndex<ColumnKey, RColumn> primaryIndexDataColumn;

  public RawBdbImpl(EntityStore store) {
    this.primaryIndex = store.getPrimaryIndex(MetadataKey.class, Metadata.class);
    this.primaryIndexDataColumn = store.getPrimaryIndex(ColumnKey.class, RColumn.class);
  }

  @Override
  public void putMetadata(byte tableId,
                          long blockId,
                          byte[] rawCTypeKeys,
                          int[] rawColIds,
                          int[] enumColIds,
                          int[] histogramColIds) {
    this.primaryIndex.putNoOverwrite(
        new Metadata(MetadataKey.builder()
                         .tableId(tableId)
                         .blockId(blockId)
                         .build(), rawCTypeKeys, rawColIds, enumColIds, histogramColIds)
    );
  }

  @Override
  public void putByte(byte tableId,
                      long blockId,
                      int[] mapping,
                      byte[][] data) {
    for (int i = 0; i < mapping.length; i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder()
              .columnKey(ColumnKey.builder().tableId(tableId).blockId(blockId).colId(mapping[i]).build())
              .dataByte(data[i]).build());
    }
  }

  @Override
  public void putInt(byte tableId,
                     long blockId,
                     int[] mapping,
                     int[][] data) {
    for (int i = 0; i < mapping.length; i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder()
              .columnKey(ColumnKey.builder().tableId(tableId).blockId(blockId).colId(mapping[i]).build())
              .dataInt(data[i]).build());
    }
  }

  @Override
  public void putLong(byte tableId,
                      long blockId,
                      int[] mapping,
                      long[][] data) {
    for (int i = 0; i < mapping.length; i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder()
              .columnKey(ColumnKey.builder().tableId(tableId).blockId(blockId).colId(mapping[i]).build())
              .dataLong(data[i]).build());
    }
  }

  @Override
  public void putFloat(byte tableId,
                       long blockId,
                       int[] mapping,
                       float[][] data) {
    for (int i = 0; i < mapping.length; i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder()
              .columnKey(ColumnKey.builder().tableId(tableId).blockId(blockId).colId(mapping[i]).build())
              .dataFloat(data[i]).build());
    }
  }

  @Override
  public void putDouble(byte tableId,
                        long blockId,
                        int[] mapping,
                        double[][] data) {
    for (int i = 0; i < mapping.length; i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder()
              .columnKey(ColumnKey.builder().tableId(tableId).blockId(blockId).colId(mapping[i]).build())
              .dataDouble(data[i]).build());
    }
  }

  @Override
  public void putString(byte tableId,
                        long blockId,
                        int[] mapping,
                        String[][] data) {
    for (int i = 0; i < mapping.length; i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder()
              .columnKey(ColumnKey.builder().tableId(tableId).blockId(blockId).colId(mapping[i]).build())
              .dataString(data[i]).build());
    }
  }

  @Override
  public void putEnum(byte tableId,
                      long blockId,
                      int[] mapping,
                      byte[][] data) {
    for (int i = 0; i < mapping.length; i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder()
              .columnKey(ColumnKey.builder().tableId(tableId).blockId(blockId).colId(mapping[i]).build())
              .dataByte(data[i]).build());
    }
  }

  @Override
  public void putCompressed(byte tableId,
                            long blockId,
                            List<Integer> rawDataTimeStampMapping,
                            List<List<Long>> rawDataTimestamp,
                            List<Integer> rawDataIntMapping,
                            List<List<Integer>> rawDataInt,
                            List<Integer> rawDataLongMapping,
                            List<List<Long>> rawDataLong,
                            List<Integer> rawDataFloatMapping,
                            List<List<Float>> rawDataFloat,
                            List<Integer> rawDataDoubleMapping,
                            List<List<Double>> rawDataDouble,
                            List<Integer> rawDataStringMapping,
                            List<List<String>> rawDataString)
      throws IOException {

    for (int i = 0; i < rawDataTimeStampMapping.size(); i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder().columnKey(
                  ColumnKey.builder().tableId(tableId).blockId(blockId).colId(rawDataTimeStampMapping.get(i)).build())
              .compressionType(CompressType.LONG)
              .dataByte(Snappy.compress(rawDataTimestamp.get(i).stream().mapToLong(j -> j).toArray())).build());
    }

    for (int i = 0; i < rawDataIntMapping.size(); i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder().columnKey(
                  ColumnKey.builder().tableId(tableId).blockId(blockId).colId(rawDataIntMapping.get(i)).build())
              .compressionType(CompressType.INT)
              .dataByte(Snappy.compress(rawDataInt.get(i).stream().mapToInt(j -> j).toArray())).build());
    }

    for (int i = 0; i < rawDataLongMapping.size(); i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder().columnKey(
                  ColumnKey.builder().tableId(tableId).blockId(blockId).colId(rawDataLongMapping.get(i)).build())
              .compressionType(CompressType.LONG)
              .dataByte(Snappy.compress(rawDataLong.get(i).stream().mapToLong(j -> j).toArray())).build());
    }

    for (int i = 0; i < rawDataFloatMapping.size(); i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder().columnKey(
                  ColumnKey.builder().tableId(tableId).blockId(blockId).colId(rawDataFloatMapping.get(i)).build())
              .compressionType(CompressType.FLOAT)
              .dataByte(Snappy.compress(rawDataFloat.get(i).stream().mapToDouble(j -> j).toArray()))
              .build());
    }

    for (int i = 0; i < rawDataDoubleMapping.size(); i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder().columnKey(
                  ColumnKey.builder().tableId(tableId).blockId(blockId).colId(rawDataDoubleMapping.get(i)).build())
              .compressionType(CompressType.DOUBLE)
              .dataByte(Snappy.compress(rawDataDouble.get(i).stream().mapToDouble(j -> j).toArray()))
              .build());
    }

    for (int i = 0; i < rawDataStringMapping.size(); i++) {
      int[] lengthArray = rawDataString.get(i).stream()
          .mapToInt(s -> (int) s.codePoints().count())
          .toArray();

      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder().columnKey(
                  ColumnKey.builder().tableId(tableId).blockId(blockId).colId(rawDataStringMapping.get(i)).build())
              .compressionType(CompressType.STRING)
              .dataInt(lengthArray)
              .dataByte(Snappy.compress(String.join("", rawDataString.get(i))))
              .build());
    }
  }

  @Override
  public void putCompressed(byte tableId,
                            long blockId,
                            CachedLastLinkedHashMap<Integer, Integer> rawDataTimeStampMapping,
                            List<List<Long>> rawDataTimestamp,
                            CachedLastLinkedHashMap<Integer, Integer> rawDataIntMapping,
                            List<List<Integer>> rawDataInt,
                            CachedLastLinkedHashMap<Integer, Integer> rawDataLongMapping,
                            List<List<Long>> rawDataLong,
                            CachedLastLinkedHashMap<Integer, Integer> rawDataFloatMapping,
                            List<List<Float>> rawDataFloat,
                            CachedLastLinkedHashMap<Integer, Integer> rawDataDoubleMapping,
                            List<List<Double>> rawDataDouble,
                            CachedLastLinkedHashMap<Integer, Integer> rawDataStringMapping,
                            List<List<String>> rawDataString) {

    rawDataTimeStampMapping.forEach((colId, i) -> {
      try {
        this.primaryIndexDataColumn.putNoOverwrite(
            RColumn.builder().columnKey(
                    ColumnKey.builder().tableId(tableId).blockId(blockId).colId(colId).build())
                .compressionType(CompressType.LONG)
                .dataByte(Snappy.compress(rawDataTimestamp.get(i).stream().mapToLong(j -> j).toArray())).build());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    rawDataIntMapping.forEach((colId, i) -> {
      try {
        this.primaryIndexDataColumn.putNoOverwrite(
            RColumn.builder().columnKey(
                    ColumnKey.builder().tableId(tableId).blockId(blockId).colId(colId).build())
                .compressionType(CompressType.INT)
                .dataByte(Snappy.compress(rawDataInt.get(i).stream().mapToInt(j -> j).toArray()))
                .build());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    rawDataLongMapping.forEach((colId, i) -> {
      try {
        this.primaryIndexDataColumn.putNoOverwrite(
            RColumn.builder().columnKey(
                    ColumnKey.builder().tableId(tableId).blockId(blockId).colId(colId).build())
                .compressionType(CompressType.LONG)
                .dataByte(Snappy.compress(rawDataLong.get(i).stream().mapToLong(j -> j).toArray()))
                .build());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    rawDataFloatMapping.forEach((colId, i) -> {
      try {
        this.primaryIndexDataColumn.putNoOverwrite(
            RColumn.builder().columnKey(
                    ColumnKey.builder().tableId(tableId).blockId(blockId).colId(colId).build())
                .compressionType(CompressType.FLOAT)
                .dataByte(Snappy.compress(rawDataFloat.get(i).stream().mapToDouble(j -> j).toArray()))
                .build());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    rawDataDoubleMapping.forEach((colId, i) -> {
      try {
        this.primaryIndexDataColumn.putNoOverwrite(
            RColumn.builder().columnKey(
                    ColumnKey.builder().tableId(tableId).blockId(blockId).colId(colId).build())
                .compressionType(CompressType.DOUBLE)
                .dataByte(Snappy.compress(rawDataDouble.get(i).stream().mapToDouble(j -> j).toArray()))
                .build());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    rawDataStringMapping.forEach((colId, i) -> {
      try {
        int[] lengthArray = rawDataString.get(i).stream()
            .mapToInt(s -> (int) s.codePoints().count())
            .toArray();

        this.primaryIndexDataColumn.putNoOverwrite(
            RColumn.builder().columnKey(
                    ColumnKey.builder().tableId(tableId).blockId(blockId).colId(colId).build())
                .compressionType(CompressType.STRING)
                .dataInt(lengthArray)
                .dataByte(Snappy.compress(String.join("", rawDataString.get(i))))
                .build());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Override
  public void putCompressed(byte tableId,
                            long blockId,
                            CachedLastLinkedHashMap<Integer, Integer> rawDataTimeStampMapping,
                            List<List<Long>> rawDataTimestamp) {
    rawDataTimeStampMapping.forEach((colId, i) -> {
      try {
        this.primaryIndexDataColumn.putNoOverwrite(
            RColumn.builder().columnKey(
                    ColumnKey.builder().tableId(tableId).blockId(blockId).colId(colId).build())
                .compressionType(CompressType.LONG)
                .dataByte(Snappy.compress(rawDataTimestamp.get(i).stream().mapToLong(j -> j).toArray())).build());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Override
  public void putCompressed(byte tableId,
                            long blockId,
                            Map<CType, Map<Integer, List<Object>>> rawDataByType) {
    for (Map.Entry<CType, Map<Integer, List<Object>>> typeEntry : rawDataByType.entrySet()) {
      CType cType = typeEntry.getKey();
      for (Map.Entry<Integer, List<Object>> colEntry : typeEntry.getValue().entrySet()) {
        int colId = colEntry.getKey();
        List<Object> dataList = colEntry.getValue();

        try {
          switch (cType) {
            case INT -> {
              int[] intArray = dataList.stream().mapToInt(o -> (Integer) o).toArray();
              byte[] compressedInts = Snappy.compress(intArray);
              primaryIndexDataColumn.putNoOverwrite(
                  RColumn.builder()
                      .columnKey(ColumnKey.builder().tableId(tableId).blockId(blockId).colId(colId).build())
                      .compressionType(CompressType.INT)
                      .dataByte(compressedInts)
                      .build());
            }
            case LONG -> {
              long[] longArray = dataList.stream().mapToLong(o -> (Long) o).toArray();
              byte[] compressedLongs = Snappy.compress(longArray);
              primaryIndexDataColumn.putNoOverwrite(
                  RColumn.builder()
                      .columnKey(ColumnKey.builder().tableId(tableId).blockId(blockId).colId(colId).build())
                      .compressionType(CompressType.LONG)
                      .dataByte(compressedLongs)
                      .build());
            }
            case FLOAT -> {
              double[] doubleArrayForFloats = new double[dataList.size()];
              for (int i = 0; i < dataList.size(); i++) {
                doubleArrayForFloats[i] = (Float) dataList.get(i);
              }
              byte[] compressedFloats = Snappy.compress(doubleArrayForFloats);
              primaryIndexDataColumn.putNoOverwrite(
                  RColumn.builder()
                      .columnKey(ColumnKey.builder().tableId(tableId).blockId(blockId).colId(colId).build())
                      .compressionType(CompressType.FLOAT)
                      .dataByte(compressedFloats)
                      .build());
            }
            case DOUBLE -> {
              double[] doubleArray = dataList.stream().mapToDouble(o -> (Double) o).toArray();
              byte[] compressedDoubles = Snappy.compress(doubleArray);
              primaryIndexDataColumn.putNoOverwrite(
                  RColumn.builder()
                      .columnKey(ColumnKey.builder().tableId(tableId).blockId(blockId).colId(colId).build())
                      .compressionType(CompressType.DOUBLE)
                      .dataByte(compressedDoubles)
                      .build());
            }
            case STRING -> {
              List<String> stringList = (List<String>) (List<?>) dataList;
              int[] lengthArray = stringList.stream()
                  .mapToInt(s -> (int) s.codePoints().count())
                  .toArray();
              String concatenated = String.join("", stringList);
              byte[] compressedString = Snappy.compress(concatenated);
              primaryIndexDataColumn.putNoOverwrite(
                  RColumn.builder()
                      .columnKey(ColumnKey.builder().tableId(tableId).blockId(blockId).colId(colId).build())
                      .compressionType(CompressType.STRING)
                      .dataInt(lengthArray)
                      .dataByte(compressedString)
                      .build());
            }
          }
        } catch (IOException e) {
          throw new RuntimeException("Compression error for column " + colId, e);
        }
      }
    }
  }

  @Override
  public byte[] getRawByte(byte tableId,
                           long blockId,
                           int colId) {
    RColumn rColumn = this.primaryIndexDataColumn.get(
        ColumnKey.builder().tableId(tableId).blockId(blockId).colId(colId).build());

    if (rColumn == null) {
      log.info("No data found for byte t::b::c -> " + tableId + "::" + blockId + "::" + colId);
      return new byte[0];
    }

    if (isNotBlockCompressed(rColumn)) {
      return rColumn.getDataByte();
    }

    try {
      return Snappy.uncompress(rColumn.getDataByte());
    } catch (Exception e) {
      log.catching(e);
    }

    return new byte[0];
  }

  @Override
  public int[] getRawInt(byte tableId,
                         long blockId,
                         int colId) {
    RColumn rColumn = this.primaryIndexDataColumn.get(
        ColumnKey.builder().tableId(tableId).blockId(blockId).colId(colId).build());

    if (rColumn == null) {
      log.info("No data found for int t::b::c -> " + tableId + "::" + blockId + "::" + colId);
      return new int[0];
    }

    if (isNotBlockCompressed(rColumn)) {
      return rColumn.getDataInt();
    }

    try {
      return Snappy.uncompressIntArray(rColumn.getDataByte());
    } catch (Exception e) {
      log.catching(e);
    }

    return new int[0];
  }

  @Override
  public long[] getRawLong(byte tableId,
                           long blockId,
                           int colId) {
    RColumn rColumn = this.primaryIndexDataColumn.get(
        ColumnKey.builder().tableId(tableId).blockId(blockId).colId(colId).build());

    if (rColumn == null) {
      log.info("No data found for long t::b::c -> " + tableId + "::" + blockId + "::" + colId);
      return new long[0];
    }

    if (isNotBlockCompressed(rColumn)) {
      return rColumn.getDataLong();
    }

    try {
      return Snappy.uncompressLongArray(rColumn.getDataByte());
    } catch (Exception e) {
      log.catching(e);
    }

    return new long[0];
  }

  @Override
  public float[] getRawFloat(byte tableId,
                             long blockId,
                             int colId) {
    RColumn rColumn = this.primaryIndexDataColumn.get(
        ColumnKey.builder().tableId(tableId).blockId(blockId).colId(colId).build());

    if (rColumn == null) {
      log.info("No data found for float t::b::c -> " + tableId + "::" + blockId + "::" + colId);
      return new float[0];
    }

    if (isNotBlockCompressed(rColumn)) {
      return rColumn.getDataFloat();
    }

    try {
      return convertDoubleArrayToFloatArray(Snappy.uncompressDoubleArray(rColumn.getDataByte()));
    } catch (Exception e) {
      log.catching(e);
    }

    return new float[0];
  }

  @Override
  public double[] getRawDouble(byte tableId,
                               long blockId,
                               int colId) {
    RColumn rColumn = this.primaryIndexDataColumn.get(
        ColumnKey.builder().tableId(tableId).blockId(blockId).colId(colId).build());

    if (rColumn == null) {
      log.info("No data found for double t::b::c -> " + tableId + "::" + blockId + "::" + colId);
      return new double[0];
    }

    if (isNotBlockCompressed(rColumn)) {
      return rColumn.getDataDouble();
    }

    try {
      return Snappy.uncompressDoubleArray(rColumn.getDataByte());
    } catch (Exception e) {
      log.catching(e);
    }

    return new double[0];
  }

  @Override
  public String[] getRawString(byte tableId,
                               long blockId,
                               int colId) {
    RColumn rColumn = this.primaryIndexDataColumn.get(
        ColumnKey.builder().tableId(tableId).blockId(blockId).colId(colId).build());

    if (rColumn == null) {
      log.info("No data found for string t::b::c -> " + tableId + "::" + blockId + "::" + colId);
      return new String[0];
    }

    if (isNotBlockCompressed(rColumn)) {
      return rColumn.getDataString();
    }

    try {
      String uncompressString = Snappy.uncompressString(rColumn.getDataByte());
      List<String> dataString = new ArrayList<>();
      AtomicLong counter = new AtomicLong(0);
      Arrays.stream(rColumn.getDataInt())
          .asLongStream()
          .forEach(l ->
                       dataString.add(uncompressString.substring((int) counter.get(), (int) (counter.addAndGet(l)))));

      return getStringFromList(dataString);
    } catch (Exception e) {
      log.catching(e);
    }

    return new String[0];
  }

  @Override
  public List<Long> getListBlockIds(byte tableId,
                                    long begin,
                                    long end) {
    List<Long> list = new ArrayList<>();

    MetadataKey beginMK = MetadataKey.builder().tableId(tableId).blockId(begin).build();
    MetadataKey endMK = MetadataKey.builder().tableId(tableId).blockId(end).build();
    EntityCursor<Metadata> cursor = doRangeQuery(this.primaryIndex, beginMK, true, endMK, true);

    try (cursor) {
      for (Metadata metadata : cursor) {
        list.add(metadata.getMetadataKey().getBlockId());
      }
    } catch (Exception e) {
      log.error(e.getMessage());
    }

    return list;
  }

  @Override
  public EntityCursor<Metadata> getMetadataEntityCursor(MetadataKey begin,
                                                        MetadataKey end) {
    return doRangeQuery(this.primaryIndex, begin, true, end, true);
  }

  @Override
  public Metadata getMetadata(MetadataKey metadataKey) {
    Metadata metadata = this.primaryIndex.get(metadataKey);

    if (metadata == null) {
      log.info("No data found for metadata key -> " + metadataKey);
      return new Metadata();
    }

    return metadata;
  }

  public String[] getStringFromList(List<String> list) {
    String[] stringArray = new String[list.size()];
    int index = 0;
    for (String b : list) {
      stringArray[index++] = b;
    }
    return stringArray;
  }

  @Override
  public long getPreviousBlockId(byte tableId,
                                 long blockId) {
    return getLastBlockIdLocal(tableId, 0L, blockId);
  }

  @Override
  public long getFirstBlockId(byte tableId,
                              int tsColId,
                              long begin,
                              long end) {
    return getFirstBlockIdLocal(tableId, tsColId, begin, end);
  }

  @Override
  public long getLastBlockId(byte tableId) {
    return getLastBlockIdLocal(tableId, 0L, Long.MAX_VALUE);
  }

  @Override
  public long getLastBlockId(byte tableId,
                             int tsColId,
                             long begin,
                             long end) {
    return getLastBlockIdLocal(tableId, tsColId, begin, end);
  }

  @Override
  public List<StackedColumn> getStacked(String tableName,
                                        CProfile tsCProfile,
                                        CProfile cProfile,
                                        GroupFunction groupFunction,
                                        CompositeFilter compositeFilter,
                                        long begin,
                                        long end) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public List<GanttColumnCount> getGanttCount(String tableName,
                                              CProfile tsCProfile,
                                              CProfile firstGrpBy,
                                              CProfile secondGrpBy,
                                              CompositeFilter compositeFilter,
                                              long begin,
                                              long end) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public List<GanttColumnSum> getGanttSum(String tableName,
                                          CProfile tsCProfile,
                                          CProfile firstGrpBy,
                                          CProfile secondGrpBy,
                                          CompositeFilter compositeFilter,
                                          long begin,
                                          long end) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public List<String> getDistinct(String tableName,
                                  CProfile tsCProfile,
                                  CProfile cProfile,
                                  OrderBy orderBy,
                                  CompositeFilter compositeFilter,
                                  int limit,
                                  long begin,
                                  long end) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public BatchResultSet getBatchResultSet(String tableName,
                                          long begin,
                                          long end,
                                          int fetchSize,
                                          List<CProfile> cProfiles) {
    throw new RuntimeException("Not supported");
  }

  private long getFirstBlockIdLocal(byte tableId,
                                    int tsColId,
                                    long begin,
                                    long end) {
    AtomicLong blockId = new AtomicLong(Long.MAX_VALUE);

    MetadataKey beginMK = MetadataKey.builder().tableId(tableId).blockId(begin).build();
    MetadataKey endMK = MetadataKey.builder().tableId(tableId).blockId(end).build();

    EntityCursor<MetadataKey> cursor = this.primaryIndex.keys(beginMK, true, endMK, true);

    try (cursor) {
      for (MetadataKey metadataKey : cursor) {
        blockId.set(Math.min(blockId.get(), metadataKey.getBlockId()));
      }
    } catch (Exception e) {
      log.error(e.getMessage());
    }

    return blockId.get();
  }

  private long getLastBlockIdLocal(byte tableId,
                                   int tsColId,
                                   long begin,
                                   long end) {
    AtomicLong blockId = new AtomicLong(Long.MIN_VALUE);

    MetadataKey beginMK = MetadataKey.builder().tableId(tableId).blockId(begin).build();
    MetadataKey endMK = MetadataKey.builder().tableId(tableId).blockId(end).build();

    EntityCursor<MetadataKey> cursor = this.primaryIndex.keys(beginMK, true, endMK, true);

    try (cursor) {
      for (MetadataKey metadataKey : cursor) {
        blockId.set(Math.max(blockId.get(), metadataKey.getBlockId()));
      }
    } catch (Exception e) {
      log.error(e.getMessage());
    }

    long[] timestamps = getRawLong(tableId, blockId.get(), tsColId);

    if (timestamps.length != 0) {
      IntStream iRow = IntStream.range(0, timestamps.length);
      iRow.forEach(iR -> {
        if (timestamps[iR] >= begin & timestamps[iR] <= end) {
          blockId.set(Math.max(blockId.get(), timestamps[iR]));
        }
      });

    }

    if (blockId.get() == Long.MIN_VALUE) {
      blockId.set(0L);
    }

    return blockId.get();
  }

  private long getLastBlockIdLocal(byte tableId,
                                   long begin,
                                   long end) {
    long lastBlockId = 0L;

    MetadataKey beginMK = MetadataKey.builder().tableId(tableId).blockId(begin).build();
    MetadataKey endMK = MetadataKey.builder().tableId(tableId).blockId(end).build();

    EntityCursor<MetadataKey> cursor
        = this.primaryIndex.keys(beginMK, true, endMK, true);

    if (cursor != null) {
      try (cursor) {
        if (cursor.last() != null) {
          lastBlockId = cursor.last().getBlockId();
        }
      }
    }

    return lastBlockId;
  }
}
