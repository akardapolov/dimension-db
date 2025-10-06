package ru.dimension.db.storage.format.write;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.cstype.SType;
import ru.dimension.db.service.store.UStore;
import ru.dimension.db.storage.format.StorageContext;
import ru.dimension.db.storage.format.StorageWriter;
import ru.dimension.db.util.CachedLastLinkedHashMap;

public class EnumStorageWriter implements StorageWriter {

  @Override
  public String getName() {
    return "ENUM_WRITER";
  }

  @Override
  public boolean supports(SType sType) {
    return SType.ENUM.equals(sType);
  }

  @Override
  public void storeData(StorageContext context, UStore uStore, CProfile cProfile) {
    int colId = cProfile.getColId();
    List<Byte> dataList = uStore.getEnumDataMap().get(colId);
    if (dataList == null || dataList.isEmpty()) {
      return;
    }

    byte tableId = context.getTableId();
    long blockId = context.getBlockId();
    boolean compression = context.isCompressionEnabled();

    CachedLastLinkedHashMap<Integer, Byte> dictionary = uStore.getEnumDictionaries().get(colId);
    if (dictionary == null || dictionary.isEmpty()) {
      // Nothing to persist without dictionary
      return;
    }

    int[] values = toIntArray(dictionary.keySet());
    byte[] data = toByteArray(dataList);

    try {
      context.getEnumDAO().putEColumn(tableId, blockId, colId, values, data, compression);
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to store ENUM data for colId=" + colId + " (" + cProfile.getColName() + ")", e
      );
    }
  }

  @Override
  public void storeBatchData(StorageContext context, UStore uStore, CProfile cProfile, int batchSize) {
    // Per-column writer; batching is effectively same as single write here
    storeData(context, uStore, cProfile);
  }

  private static int[] toIntArray(Set<Integer> set) {
    int[] arr = new int[set.size()];
    int i = 0;
    for (Integer v : set) {
      arr[i++] = (v == null) ? 0 : v;
    }
    return arr;
  }

  private static byte[] toByteArray(List<Byte> list) {
    byte[] arr = new byte[list.size()];
    for (int i = 0; i < list.size(); i++) {
      Byte b = list.get(i);
      arr[i] = b == null ? 0 : b;
    }
    return arr;
  }
}