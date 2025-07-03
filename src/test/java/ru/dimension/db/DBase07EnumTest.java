package ru.dimension.db;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ru.dimension.db.exception.EnumByteExceedException;
import ru.dimension.db.storage.helper.EnumHelper;
import ru.dimension.db.util.CachedLastLinkedHashMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class DBase07EnumTest {

  @Test
  public void fillMapTest() throws EnumByteExceedException {
    CachedLastLinkedHashMap<Integer, Byte> values = new CachedLastLinkedHashMap<>();

    byte byteValueFirst = EnumHelper.getByteValue(values, 123);
    assertEquals(Byte.MIN_VALUE, byteValueFirst);

    byte byteValueSame = EnumHelper.getByteValue(values, 123);
    assertEquals(Byte.MIN_VALUE, byteValueSame);

    byte byteValueNext = EnumHelper.getByteValue(values, 1234);
    assertEquals(Byte.MIN_VALUE + 1, byteValueNext);
  }

}
