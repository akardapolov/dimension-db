package ru.dimension.db.service;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import ru.dimension.db.exception.BeginEndWrongOrderException;
import ru.dimension.db.exception.SqlColMetadataException;
import ru.dimension.db.model.output.BlockKeyTail;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.sql.BatchResultSet;

public interface RawService {

  List<List<Object>> getRawDataAll(String tableName,
                                   long begin,
                                   long end);

  List<List<Object>> getRawDataAll(String tableName,
                                   CProfile cProfileFilter,
                                   String filter,
                                   long begin,
                                   long end);

  List<List<Object>> getRawDataByColumn(String tableName,
                                        CProfile cProfile,
                                        long begin,
                                        long end);

  BatchResultSet getBatchResultSet(String tableName,
                                   long begin,
                                   long end,
                                   int fetchSize);

  Entry<Entry<Long, Integer>, List<Object>> getColumnData(byte tableId,
                                                          int colIndex,
                                                          int tsColIndex,
                                                          CProfile cProfile,
                                                          int fetchSize,
                                                          boolean isStarted,
                                                          long maxBlockId,
                                                          Entry<Long, Integer> pointer,
                                                          AtomicInteger fetchCounter);

  long getMaxBlockId(byte tableId);

  long getFirst(String tableName,
                long begin,
                long end);

  long getLast(String tableName,
               long begin,
               long end);

  List<BlockKeyTail> getBlockKeyTailList(String tableName,
                                         long begin,
                                         long end) throws BeginEndWrongOrderException, SqlColMetadataException;
}
