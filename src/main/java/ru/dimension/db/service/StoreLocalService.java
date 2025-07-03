package ru.dimension.db.service;

import java.sql.ResultSet;
import java.util.List;

public interface StoreLocalService {

  void putDataDirect(String tableName,
                     List<List<Object>> data);

  long putDataJdbc(String tableName,
                   ResultSet resultSet);

  void putDataJdbcBatch(String tableName,
                        ResultSet resultSet,
                        Integer fBaseBatchSize);

  void putDataCsvBatch(String tableName,
                       String fileName,
                       String csvSplitBy,
                       Integer fBaseBatchSize);
}
