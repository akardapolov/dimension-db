package ru.dimension.db.storage.jdbc;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.dbcp2.BasicDataSource;
import ru.dimension.db.storage.HistogramDAO;

@Log4j2
public class HistogramJdbcImpl implements HistogramDAO {

  private final BasicDataSource basicDataSource;

  public HistogramJdbcImpl(BasicDataSource basicDataSource) {
    this.basicDataSource = basicDataSource;
  }

  @Override
  public void put(byte tableId,
                  long blockId,
                  int colId,
                  int[][] data) {
    throw new UnsupportedOperationException("Not supported for JDBC");
  }

  @Override
  public void putCompressed(byte tableId,
                            long blockId,
                            int colId,
                            int[][] data) {
    throw new UnsupportedOperationException("Not supported for JDBC");
  }

  @Override
  public void putCompressedKeysValues(byte tableId,
                                      long blockId,
                                      int colId,
                                      int[] keys,
                                      int[] values) {
    throw new UnsupportedOperationException("Not supported for JDBC");
  }

  @Override
  public int[][] get(byte tableId,
                     long blockId,
                     int colId) {
    throw new UnsupportedOperationException("Not supported for JDBC");
  }
}