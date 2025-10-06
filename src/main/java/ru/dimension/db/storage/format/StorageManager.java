package ru.dimension.db.storage.format;

import java.util.Map;

import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.cstype.SType;
import ru.dimension.db.service.store.UStore;
import ru.dimension.db.storage.format.read.EnumStorageReader;
import ru.dimension.db.storage.format.registry.StorageReaderRegistry;
import ru.dimension.db.storage.format.registry.StorageWriterRegistry;
import ru.dimension.db.storage.format.write.EnumStorageWriter;
import ru.dimension.db.storage.format.read.HistogramStorageReader;
import ru.dimension.db.storage.format.write.HistogramStorageWriter;
import ru.dimension.db.storage.format.read.RawStorageReader;
import ru.dimension.db.storage.format.write.RawStorageWriter;

public class StorageManager {
  private final StorageReaderRegistry readRegistry;
  private final StorageWriterRegistry writeRegistry;

  public StorageManager() {
    this.readRegistry = initializeReadRegistry();
    this.writeRegistry = initializeWriteRegistry();
  }

  private StorageReaderRegistry initializeReadRegistry() {
    StorageReaderRegistry registry = new StorageReaderRegistry();
    registry.registerFormat(new RawStorageReader());
    registry.registerFormat(new EnumStorageReader());
    registry.registerFormat(new HistogramStorageReader());
    return registry;
  }

  private StorageWriterRegistry initializeWriteRegistry() {
    StorageWriterRegistry registry = new StorageWriterRegistry();
    registry.registerWriter(new RawStorageWriter());
    registry.registerWriter(new EnumStorageWriter());
    registry.registerWriter(new HistogramStorageWriter());
    return registry;
  }

  // Read operations
  public String[] readStringValues(StorageContext context, CProfile cProfile, SType sType) {
    StorageReader format = readRegistry.getFormat(sType);
    return format.getStringValues(context, cProfile);
  }

  public double[] readDoubleValues(StorageContext context, CProfile cProfile, SType sType) {
    StorageReader format = readRegistry.getFormat(sType);
    return format.getDoubleValues(context, cProfile);
  }

  public int[] readIntValues(StorageContext context, CProfile cProfile, SType sType) {
    StorageReader format = readRegistry.getFormat(sType);
    return format.getIntValues(context, cProfile);
  }

  // Write operations
  public void writeData(StorageContext context, UStore uStore, CProfile cProfile, SType sType) {
    StorageWriter writer = writeRegistry.getWriter(sType);
    writer.storeData(context, uStore, cProfile);
  }

  public void writeBatchData(StorageContext context, UStore uStore, CProfile cProfile, SType sType, int batchSize) {
    StorageWriter writer = writeRegistry.getWriter(sType);
    writer.storeBatchData(context, uStore, cProfile, batchSize);
  }

  // Bulk operations
  public void writeAllData(StorageContext context, UStore uStore, Map<CProfile, SType> profileStorageMap) {
    for (Map.Entry<CProfile, SType> entry : profileStorageMap.entrySet()) {
      writeData(context, uStore, entry.getKey(), entry.getValue());
    }
  }

  // Registry access
  public StorageReaderRegistry getReadRegistry() {
    return readRegistry;
  }

  public StorageWriterRegistry getWriteRegistry() {
    return writeRegistry;
  }
}