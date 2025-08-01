package ru.dimension.db.storage.oracle;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.dbcp2.BasicDataSource;
import ru.dimension.db.storage.DimensionDAO;

@Log4j2
public class DimensionOracleImpl implements DimensionDAO {

  private final BasicDataSource basicDataSource;

  public DimensionOracleImpl(BasicDataSource basicDataSource) {
    this.basicDataSource = basicDataSource;
  }

  @Override
  public int getOrLoad(double value) {
    throw new UnsupportedOperationException("Not supported implementation");
  }

  @Override
  public int getOrLoad(String value) {
    throw new UnsupportedOperationException("Not supported implementation");
  }

  @Override
  public int getOrLoad(long value) {
    throw new UnsupportedOperationException("Not supported implementation");
  }

  @Override
  public String getStringById(int key) {
    throw new UnsupportedOperationException("Not supported implementation");
  }

  @Override
  public double getDoubleById(int key) {
    throw new UnsupportedOperationException("Not supported implementation");
  }

  @Override
  public long getLongById(int key) {
    throw new UnsupportedOperationException("Not supported implementation");
  }
}