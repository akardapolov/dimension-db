package ru.dimension.db.storage.format.write;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.cstype.CType;
import ru.dimension.db.model.profile.cstype.SType;
import ru.dimension.db.service.store.UStore;
import ru.dimension.db.storage.format.StorageContext;
import ru.dimension.db.storage.format.StorageWriter;

public class RawStorageWriter implements StorageWriter {

  @Override
  public String getName() {
    return "RAW_WRITER";
  }

  @Override
  public boolean supports(SType sType) {
    return SType.RAW.equals(sType);
  }

  @Override
  public void storeData(StorageContext context, UStore uStore, CProfile cProfile) {
    int colId = cProfile.getColId();
    List<?> data = uStore.getRawDataMap().get(colId);
    if (data == null || data.isEmpty()) {
      return;
    }

    byte tableId = context.getTableId();
    long blockId = context.getBlockId();
    boolean compression = context.isCompressionEnabled();

    CType cType = cProfile.getCsType().getCType();

    try {
      if (compression) {
        // Build a single-column, single-type map and write via DAO
        Map<Integer, List<Object>> colDataMap = new HashMap<>(1);
        //noinspection unchecked
        colDataMap.put(colId, (List<Object>) data);
        Map<CType, Map<Integer, List<Object>>> byType = new EnumMap<>(CType.class);
        byType.put(cType, colDataMap);
        context.getRawDAO().putCompressed(tableId, blockId, byType);
        return;
      }

      int[] colIds = new int[]{colId};
      switch (cType) {
        case INT: {
          int[] arr = toIntArray(data);
          context.getRawDAO().putInt(tableId, blockId, colIds, new int[][]{arr});
          break;
        }
        case LONG: {
          long[] arr = toLongArray(data);
          context.getRawDAO().putLong(tableId, blockId, colIds, new long[][]{arr});
          break;
        }
        case FLOAT: {
          float[] arr = toFloatArray(data);
          context.getRawDAO().putFloat(tableId, blockId, colIds, new float[][]{arr});
          break;
        }
        case DOUBLE: {
          double[] arr = toDoubleArray(data);
          context.getRawDAO().putDouble(tableId, blockId, colIds, new double[][]{arr});
          break;
        }
        case STRING: {
          String[] arr = toStringArray(data);
          context.getRawDAO().putString(tableId, blockId, colIds, new String[][]{arr});
          break;
        }
        default:
          // No-op for unsupported types
      }
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to store RAW data for colId=" + colId + " (" + cProfile.getColName() + ")", e
      );
    }
  }

  @Override
  public void storeBatchData(StorageContext context, UStore uStore, CProfile cProfile, int batchSize) {
    // For now, per-column write doesn't need special batch handling.
    storeData(context, uStore, cProfile);
  }

  private static int[] toIntArray(List<?> list) {
    int[] arr = new int[list.size()];
    for (int i = 0; i < list.size(); i++) {
      Object o = list.get(i);
      arr[i] = (o == null) ? 0 : ((Number) o).intValue();
    }
    return arr;
  }

  private static long[] toLongArray(List<?> list) {
    long[] arr = new long[list.size()];
    for (int i = 0; i < list.size(); i++) {
      Object o = list.get(i);
      arr[i] = (o == null) ? 0L : ((Number) o).longValue();
    }
    return arr;
  }

  private static float[] toFloatArray(List<?> list) {
    float[] arr = new float[list.size()];
    for (int i = 0; i < list.size(); i++) {
      Object o = list.get(i);
      arr[i] = (o == null) ? 0f : ((Number) o).floatValue();
    }
    return arr;
  }

  private static double[] toDoubleArray(List<?> list) {
    double[] arr = new double[list.size()];
    for (int i = 0; i < list.size(); i++) {
      Object o = list.get(i);
      arr[i] = (o == null) ? 0d : ((Number) o).doubleValue();
    }
    return arr;
  }

  private static String[] toStringArray(List<?> list) {
    String[] arr = new String[list.size()];
    for (int i = 0; i < list.size(); i++) {
      Object o = list.get(i);
      arr[i] = (o == null) ? null : String.valueOf(o);
    }
    return arr;
  }
}