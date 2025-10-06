package ru.dimension.db.storage.format.registry;

import ru.dimension.db.model.profile.cstype.SType;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import ru.dimension.db.storage.format.StorageWriter;

public class StorageWriterRegistry {
  private final Map<String, StorageWriter> writersByName = new ConcurrentHashMap<>();
  private final Map<SType, StorageWriter> writersByType = new ConcurrentHashMap<>();

  public void registerWriter(StorageWriter writer) {
    writersByName.put(writer.getName(), writer);

    if (writer.supports(SType.RAW)) writersByType.put(SType.RAW, writer);
    if (writer.supports(SType.ENUM)) writersByType.put(SType.ENUM, writer);
    if (writer.supports(SType.HISTOGRAM)) writersByType.put(SType.HISTOGRAM, writer);
  }

  public StorageWriter getWriter(SType sType) {
    StorageWriter writer = writersByType.get(sType);
    if (writer == null) {
      throw new IllegalArgumentException("No storage writer registered for type: " + sType);
    }
    return writer;
  }

  public StorageWriter getWriter(String name) {
    StorageWriter writer = writersByName.get(name);
    if (writer == null) {
      throw new IllegalArgumentException("No storage writer registered with name: " + name);
    }
    return writer;
  }

  public Set<StorageWriter> getAllWriters() {
    return new HashSet<>(writersByName.values());
  }
}