package ru.dimension.db.storage.format.registry;

import ru.dimension.db.model.profile.cstype.SType;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import ru.dimension.db.storage.format.StorageReader;

public class StorageReaderRegistry {
  private final Map<String, StorageReader> formatsByName = new ConcurrentHashMap<>();
  private final Map<SType, StorageReader> formatsByType = new ConcurrentHashMap<>();

  public void registerFormat(StorageReader format) {
    formatsByName.put(format.getName(), format);

    if (format.supports(SType.RAW)) formatsByType.put(SType.RAW, format);
    if (format.supports(SType.ENUM)) formatsByType.put(SType.ENUM, format);
    if (format.supports(SType.HISTOGRAM)) formatsByType.put(SType.HISTOGRAM, format);
  }

  public StorageReader getFormat(SType sType) {
    StorageReader format = formatsByType.get(sType);
    if (format == null) {
      throw new IllegalArgumentException("No storage format registered for type: " + sType);
    }
    return format;
  }

  public StorageReader getFormat(String name) {
    StorageReader format = formatsByName.get(name);
    if (format == null) {
      throw new IllegalArgumentException("No storage format registered with name: " + name);
    }
    return format;
  }

  public Set<StorageReader> getAllFormats() {
    return new HashSet<>(formatsByName.values());
  }
}