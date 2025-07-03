package ru.dimension.db.storage;

import java.io.IOException;
import ru.dimension.db.storage.bdb.entity.column.EColumn;

public interface EnumDAO {

  EColumn putEColumn(byte tableId,
                     long blockId,
                     int colId,
                     int[] values,
                     byte[] data,
                     boolean compression) throws IOException;

  EColumn getEColumnValues(byte tableId,
                           long blockId,
                           int colId);
}
