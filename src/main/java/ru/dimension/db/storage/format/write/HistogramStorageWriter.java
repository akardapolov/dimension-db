package ru.dimension.db.storage.format.write;

import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.cstype.SType;
import ru.dimension.db.service.store.HEntry;
import ru.dimension.db.service.store.UStore;
import ru.dimension.db.storage.format.StorageContext;
import ru.dimension.db.storage.format.StorageWriter;

public class HistogramStorageWriter implements StorageWriter {

  @Override
  public String getName() {
    return "HISTOGRAM_WRITER";
  }

  @Override
  public boolean supports(SType sType) {
    return SType.HISTOGRAM.equals(sType);
  }

  @Override
  public void storeData(StorageContext context, UStore uStore, CProfile cProfile) {
    int colId = cProfile.getColId();
    HEntry h = uStore.getHistogramDataMap().get(colId);
    if (h == null || h.getSize() <= 0) {
      return;
    }

    byte tableId = context.getTableId();
    long blockId = context.getBlockId();
    boolean compression = context.isCompressionEnabled();

    if (compression) {
      context.getHistogramDAO().putCompressedKeysValues(tableId, blockId, colId, h.getIndices(), h.getValues());
    } else {
      // Equivalent of StoreServiceImpl.getArrayFromMapHEntry(h)
      int[][] pairs = new int[][]{h.getIndices(), h.getValues()};
      context.getHistogramDAO().put(tableId, blockId, colId, pairs);
    }
  }

  @Override
  public void storeBatchData(StorageContext context, UStore uStore, CProfile cProfile, int batchSize) {
    // Per-column writer; batching is effectively same as single write here
    storeData(context, uStore, cProfile);
  }
}