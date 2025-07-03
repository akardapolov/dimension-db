package ru.dimension.db.storage.oracle;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.dbcp2.BasicDataSource;
import ru.dimension.db.storage.EnumDAO;
import ru.dimension.db.storage.bdb.QueryBdbApi;
import ru.dimension.db.storage.bdb.entity.column.EColumn;

@Log4j2
public class EnumOracleImpl extends QueryBdbApi implements EnumDAO {

  private final BasicDataSource basicDataSource;

  public EnumOracleImpl(BasicDataSource basicDataSource) {
    this.basicDataSource = basicDataSource;
  }

  @Override
  public EColumn putEColumn(byte tableId,
                            long blockId,
                            int colId,
                            int[] values,
                            byte[] data,
                            boolean compression) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public EColumn getEColumnValues(byte tableId,
                                  long blockId,
                                  int colId) {
    throw new RuntimeException("Not supported");
  }
}
