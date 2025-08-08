package ru.dimension.db.storage.bdb.impl;

import com.sleepycat.je.UniqueConstraintException;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.log4j.Log4j2;
import ru.dimension.db.storage.DimensionDAO;
import ru.dimension.db.storage.bdb.entity.dictionary.DIntDouble;
import ru.dimension.db.storage.bdb.entity.dictionary.DIntLong;
import ru.dimension.db.storage.bdb.entity.dictionary.DIntString;

@Log4j2
public class DimensionBdbImpl implements DimensionDAO {
  private AtomicInteger longIdCounter;
  private AtomicInteger doubleIdCounter;
  private AtomicInteger stringIdCounter;

  private final SecondaryIndex<Long, Integer, DIntLong> secondaryIndexLong;
  private final SecondaryIndex<Double, Integer, DIntDouble> secondaryIndexDouble;
  private final SecondaryIndex<String, Integer, DIntString> secondaryIndexString;
  private final PrimaryIndex<Integer, DIntLong> primaryIndexLong;
  private final PrimaryIndex<Integer, DIntDouble> primaryIndexDouble;
  private final PrimaryIndex<Integer, DIntString> primaryIndexString;

  public DimensionBdbImpl(EntityStore store) {
    this.primaryIndexLong = store.getPrimaryIndex(Integer.class, DIntLong.class);
    this.secondaryIndexLong = store.getSecondaryIndex(primaryIndexLong, Long.class, "value");

    this.primaryIndexDouble = store.getPrimaryIndex(Integer.class, DIntDouble.class);
    this.secondaryIndexDouble = store.getSecondaryIndex(primaryIndexDouble, Double.class, "value");

    this.primaryIndexString = store.getPrimaryIndex(Integer.class, DIntString.class);
    this.secondaryIndexString = store.getSecondaryIndex(primaryIndexString, String.class, "value");

    this.longIdCounter = new AtomicInteger((int) (primaryIndexLong.count() + 1));
    this.doubleIdCounter = new AtomicInteger((int) (primaryIndexDouble.count() + 1));
    this.stringIdCounter = new AtomicInteger((int) (primaryIndexString.count() + 1));
  }

  @Override
  public int getOrLoad(double value) {
    // First try: Fast-path for existing values
    DIntDouble entity = secondaryIndexDouble.get(value);
    if (entity != null) {
      return entity.getParam();
    }

    // Second try: Attempt insertion with new ID
    int newId = doubleIdCounter.incrementAndGet();
    try {
      primaryIndexDouble.putNoReturn(new DIntDouble(newId, value));
      return newId;
    } catch (UniqueConstraintException e) {
      // Collision: Value added concurrently
      return secondaryIndexDouble.get(value).getParam();
    }
  }

  @Override
  public int getOrLoad(String value) {
    // First try: Fast-path for existing values
    DIntString entity = secondaryIndexString.get(value);
    if (entity != null) {
      return entity.getParam();
    }

    // Second try: Attempt insertion with new ID
    int newId = stringIdCounter.incrementAndGet();
    try {
      primaryIndexString.putNoReturn(new DIntString(newId, value));
      return newId;
    } catch (UniqueConstraintException e) {
      // Collision: Value added concurrently
      return secondaryIndexString.get(value).getParam();
    }
  }

  @Override
  public int getOrLoad(long value) {
    // First try: Fast-path for existing values
    DIntLong entity = secondaryIndexLong.get(value);
    if (entity != null) {
      return entity.getParam();
    }

    // Second try: Attempt insertion with new ID
    int newId = longIdCounter.incrementAndGet();
    try {
      primaryIndexLong.putNoReturn(new DIntLong(newId, value));
      return newId;
    } catch (UniqueConstraintException e) {
      // Collision: Value added concurrently
      return secondaryIndexLong.get(value).getParam();
    }
  }

  @Override
  public String getStringById(int key) {
    return this.primaryIndexString.get(key).getValue();
  }

  @Override
  public double getDoubleById(int key) {
    return this.primaryIndexDouble.get(key).getValue();
  }

  @Override
  public long getLongById(int key) {
    return this.primaryIndexLong.get(key).getValue();
  }
}
