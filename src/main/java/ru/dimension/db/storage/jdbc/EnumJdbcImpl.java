package ru.dimension.db.storage.jdbc;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.dbcp2.BasicDataSource;
import ru.dimension.db.storage.EnumDAO;
import ru.dimension.db.storage.bdb.entity.column.EColumn;

@Log4j2
public class EnumJdbcImpl implements EnumDAO {

  private final BasicDataSource basicDataSource;

  public EnumJdbcImpl(BasicDataSource basicDataSource) {
    this.basicDataSource = basicDataSource;
  }

  @Override
  public EColumn putEColumn(byte tableId,
                            long blockId,
                            int colId,
                            int[] values,
                            byte[] data,
                            boolean compression) {
    throw new UnsupportedOperationException("Not supported for JDBC");
  }

  @Override
  public EColumn getEColumnValues(byte tableId,
                                  long blockId,
                                  int colId) {
    throw new UnsupportedOperationException("Not supported for JDBC");
  }
}