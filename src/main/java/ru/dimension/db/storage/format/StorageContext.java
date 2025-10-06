package ru.dimension.db.storage.format;

import lombok.Builder;
import lombok.Getter;
import ru.dimension.db.storage.Converter;
import ru.dimension.db.storage.EnumDAO;
import ru.dimension.db.storage.HistogramDAO;
import ru.dimension.db.storage.RawDAO;
import ru.dimension.db.storage.format.registry.StorageReaderRegistry;
import ru.dimension.db.storage.format.registry.StorageWriterRegistry;

@Getter
@Builder
public class StorageContext {
  private final byte tableId;
  private final long blockId;
  private final long[] timestamps;
  private final RawDAO rawDAO;
  private final EnumDAO enumDAO;
  private final HistogramDAO histogramDAO;
  private final Converter converter;
  private final boolean compressionEnabled;

  private final StorageReaderRegistry readRegistry;
  private final StorageWriterRegistry writeRegistry;
}