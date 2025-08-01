package ru.dimension.db;

import java.util.LinkedHashMap;
import ru.dimension.db.common.AbstractH2Test;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.cstype.SType;
import ru.dimension.db.model.profile.table.AType;
import ru.dimension.db.model.profile.table.IType;
import ru.dimension.db.model.profile.table.TType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DBase04RawIndexLocalJdbcTest extends AbstractH2Test {

  private List<List<Object>> expected = new ArrayList<>();

  @BeforeAll
  public void init() {
    loadExpected(expected);

    Map<String, SType> csTypeMap = new LinkedHashMap<>();
    csTypeMap.put("ID", SType.RAW);

    putDataJdbc(csTypeMap, TType.TIME_SERIES, IType.LOCAL, AType.ON_LOAD,false);
  }

  @Test
  public void computeTableRawDataBeginEnd012Test() {
    List<List<Object>> actual = getRawDataAll(0, 12);

    assertEquals(expected.size(), actual.size());
    assertForRaw(expected, actual);
  }

  @Test
  public void computeTableRawDataBeginEnd77Test() {
    List<List<Object>> actual = getRawDataAll(7, 7);

    assertEquals(expected.stream().filter(e -> e.get(0) == "7").count(), actual.size());
    assertForRaw(expected.stream().filter(e -> e.get(0) == "7").collect(Collectors.toList()),
        actual);
  }

  @Test
  public void computeTableRawDataBeginEnd57Test() {
    List<List<Object>> actual = getRawDataAll(5, 7);

    Predicate<List<Object>> filter = e -> (e.get(0) == "5" | e.get(0) == "6" | e.get(0) == "7");

    assertEquals(expected.stream().filter(filter).count(), actual.size());
    assertForRaw(expected.stream().filter(filter).collect(Collectors.toList()), actual);
  }

  @Test
  public void computeTableRawDataBeginEnd67Test() {
    List<List<Object>> actual = getRawDataAll(6, 7);

    Predicate<List<Object>> filter = e -> (e.get(0) == "6" | e.get(0) == "7");

    assertEquals(expected.stream().filter(filter).count(), actual.size());
    assertForRaw(expected.stream().filter(filter).collect(Collectors.toList()), actual);
  }

  @Test
  public void computeTableRawDataBeginEnd16Test() {
    List<List<Object>> actual = getRawDataAll(1, 6);

    Predicate<List<Object>> filter = e -> (e.get(0) == "1" | e.get(0) == "2" | e.get(0) == "3"
        | e.get(0) == "4" | e.get(0) == "5" | e.get(0) == "6");

    assertEquals(expected.stream().filter(filter).count(), actual.size());
    assertForRaw(expected.stream().filter(filter).collect(Collectors.toList()), actual);
  }

  @Test
  public void computeTableRawDataBeginEnd811Test() {
    List<List<Object>> actual = getRawDataAll(8, 11);

    Predicate<List<Object>> filter = e -> (e.get(0) == "8" | e.get(0) == "9"
        | e.get(0) == "10" | e.get(0) == "11");

    assertEquals(expected.stream().filter(filter).count(), actual.size());
    assertForRaw(expected.stream().filter(filter).collect(Collectors.toList()), actual);
  }

  @Test
  public void computeTableRawDataByColumnIdBeginEnd811Test() {
    CProfile cProfile = getCProfileByColumnName("ID");

    List<List<Object>> actual = getRawDataByColumn(cProfile, 8, 11);

    Predicate<List<Object>> filter = e -> (e.get(0) == "8" | e.get(0) == "9"
        | e.get(0) == "10" | e.get(0) == "11");

    assertEquals(expected.stream().filter(filter).count(), actual.size());
    assertForRaw(expected.stream().filter(filter).map(map -> List.of(map.get(0), map.get(0))).collect(Collectors.toList()), actual);
  }

  @Test
  public void computeTableRawDataByColumnFirstNameBeginEnd811Test() {
    CProfile cProfile = getCProfileByColumnName("FIRSTNAME");
    List<List<Object>> actual = getRawDataByColumn(cProfile, 8, 11);

    Predicate<List<Object>> filter = e -> (e.get(0) == "8" | e.get(0) == "9"
        | e.get(0) == "10" | e.get(0) == "11");

    assertEquals(expected.stream().filter(filter).count(), actual.size());
    assertForRaw(expected.stream().filter(filter).map(map -> List.of(map.get(0), map.get(1))).collect(Collectors.toList()), actual);
  }

  @Test
  public void computeTableRawDataByColumnCityBeginEnd811Test() {
    CProfile cProfile = getCProfileByColumnName("CITY");
    List<List<Object>> actual = getRawDataByColumn(cProfile, 8, 11);

    Predicate<List<Object>> filter = e -> (e.get(0) == "8" | e.get(0) == "9"
        | e.get(0) == "10" | e.get(0) == "11");

    assertEquals(expected.stream().filter(filter).count(), actual.size());
    assertForRaw(expected.stream().filter(filter).map(map -> List.of(map.get(0), map.get(4))).collect(Collectors.toList()), actual);
  }

  @Test
  public void computeTableRawDataTimestampTest() {
    List<List<Object>> expectedLocal = expected.stream().filter(f -> f.get(0).equals("1")).toList();
    List<List<Object>> actual = getRawDataAll(1, 1);

    assertEquals(expectedLocal.size(), actual.size());
    assertForRaw(expectedLocal, actual);
  }

}
