package ru.dimension.db.storage.format;

import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.cstype.SType;
import ru.dimension.db.service.store.UStore;

public interface StorageWriter {
  String getName();
  boolean supports(SType sType);

  void storeData(StorageContext context, UStore uStore, CProfile cProfile);
  void storeBatchData(StorageContext context, UStore uStore, CProfile cProfile, int batchSize);
}