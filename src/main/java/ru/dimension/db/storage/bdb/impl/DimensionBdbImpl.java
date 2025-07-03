package ru.dimension.db.storage.bdb.impl;

import com.sleepycat.je.UniqueConstraintException;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;
import lombok.extern.log4j.Log4j2;
import ru.dimension.db.storage.DimensionDAO;
import ru.dimension.db.storage.bdb.entity.dictionary.DIntDouble;
import ru.dimension.db.storage.bdb.entity.dictionary.DIntString;

@Log4j2
public class DimensionBdbImpl implements DimensionDAO {

  private final SecondaryIndex<Double, Integer, DIntDouble> secondaryIndexDouble;
  private final SecondaryIndex<String, Integer, DIntString> secondaryIndexString;
  private final PrimaryIndex<Integer, DIntDouble> primaryIndexDouble;
  private final PrimaryIndex<Integer, DIntString> primaryIndexString;

  public DimensionBdbImpl(EntityStore store) {
    this.primaryIndexDouble = store.getPrimaryIndex(Integer.class, DIntDouble.class);
    this.secondaryIndexDouble = store.getSecondaryIndex(primaryIndexDouble, Double.class, "value");

    this.primaryIndexString = store.getPrimaryIndex(Integer.class, DIntString.class);
    this.secondaryIndexString = store.getSecondaryIndex(primaryIndexString, String.class, "value");
  }

  @Override
  public int getOrLoad(double value) {
    if (!secondaryIndexDouble.contains(value)) {
      try {
        this.primaryIndexDouble.putNoReturn(new DIntDouble(0, value));
      } catch (UniqueConstraintException e) {
        log.info("Value: {} already exists", value);
        return this.secondaryIndexDouble.get(value).getParam();
      }
    }
    return this.secondaryIndexDouble.get(value).getParam();
  }

  @Override
  public int getOrLoad(String value) {
    if (!secondaryIndexString.contains(value)) {
      try {
        this.primaryIndexString.putNoReturn(new DIntString(0, value));
      } catch (UniqueConstraintException e) {
        log.info("Value: {} already exists", value);
        return this.secondaryIndexString.get(value).getParam();
      }
    }
    return this.secondaryIndexString.get(value).getParam();
  }

  @Override
  public String getStringById(int key) {
    return this.primaryIndexString.get(key).getValue();
  }

  @Override
  public double getDoubleById(int key) {
    return this.primaryIndexDouble.get(key).getValue();
  }
}
