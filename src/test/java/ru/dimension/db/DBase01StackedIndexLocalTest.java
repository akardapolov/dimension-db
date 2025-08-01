package ru.dimension.db;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import ru.dimension.db.common.AbstractH2Test;
import ru.dimension.db.exception.BeginEndWrongOrderException;
import ru.dimension.db.exception.SqlColMetadataException;
import ru.dimension.db.model.output.StackedColumn;
import ru.dimension.db.model.profile.cstype.SType;
import ru.dimension.db.model.profile.table.AType;
import ru.dimension.db.model.profile.table.IType;
import ru.dimension.db.model.profile.table.TType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class DBase01StackedIndexLocalTest extends AbstractH2Test {

  @BeforeAll
  public void init() {
    Map<String, SType> csTypeMap = new LinkedHashMap<>();
    csTypeMap.put("ID", SType.RAW);

    putDataJdbc(csTypeMap, TType.TIME_SERIES, IType.LOCAL, AType.ON_LOAD, true);
  }

  @Test
  public void computeBeginEnd12Test() throws BeginEndWrongOrderException, SqlColMetadataException {
    List<StackedColumn> listIndexed = getDataStackedColumn("LASTNAME", 1, 2);
    List<StackedColumn> listNotIndexed = getDataStackedColumn("FIRSTNAME", 1, 2);

    assertEquals(firstListStackedKey(listIndexed), "Ivanov");
    assertEquals(firstListStackedValue(listIndexed), 2);

    assertEquals(firstListStackedKey(listNotIndexed), "Alex");
    assertEquals(firstListStackedValue(listNotIndexed), 1);

    assertEquals(lastListStackedKey(listIndexed), "Ivanov");
    assertEquals(lastListStackedKey(listNotIndexed), "Ivan");
  }

  @Test
  public void computeBeginEnd23Test() throws BeginEndWrongOrderException, SqlColMetadataException {
    List<StackedColumn> listIndexed = getDataStackedColumn("LASTNAME", 2, 3);
    List<StackedColumn> listNotIndexed = getDataStackedColumn("FIRSTNAME", 2, 3);

    assertEquals(firstListStackedKey(listIndexed), "Ivanov");
    assertEquals(firstListStackedValue(listIndexed), 1);

    assertEquals(firstListStackedKey(listNotIndexed), "Ivan");
    assertEquals(firstListStackedValue(listNotIndexed), 1);

    assertEquals(lastListStackedKey(listIndexed), "Petrov");
    assertEquals(lastListStackedKey(listNotIndexed), "Oleg");
  }

  @Test
  public void computeBeginEnd34Test() throws BeginEndWrongOrderException, SqlColMetadataException {
    List<StackedColumn> listIndexed = getDataStackedColumn("LASTNAME", 3, 4);
    List<StackedColumn> listNotIndexed = getDataStackedColumn("FIRSTNAME", 3, 4);

    assertEquals(firstListStackedKey(listIndexed), "Petrov");
    assertEquals(firstListStackedValue(listIndexed), 1);

    assertEquals(firstListStackedKey(listNotIndexed), "Oleg");
    assertEquals(firstListStackedValue(listNotIndexed), 1);

    assertEquals(lastListStackedKey(listIndexed), "Sui");
    assertEquals(lastListStackedKey(listNotIndexed), "Lee");
  }

  @Test
  public void computeBeginEnd79Test() throws BeginEndWrongOrderException, SqlColMetadataException {
    List<StackedColumn> listIndexed = getDataStackedColumn("LASTNAME", 7, 9);
    List<StackedColumn> listNotIndexed = getDataStackedColumn("FIRSTNAME", 7, 9);

    assertEquals(firstListStackedKey(listIndexed), "Petrov");
    assertEquals(firstListStackedValue(listIndexed), 1);

    assertEquals(firstListStackedKey(listNotIndexed), "Men");
    assertEquals(firstListStackedValue(listNotIndexed), 1);

    assertEquals(lastListStackedKey(listIndexed), "Шаляпин");
    assertEquals(lastListStackedKey(listNotIndexed), "Федор");
  }

  @Test
  public void computeBeginEnd59Test() throws BeginEndWrongOrderException, SqlColMetadataException {
    List<StackedColumn> listIndexed = getDataStackedColumn("LASTNAME", 5, 9);
    List<StackedColumn> listNotIndexed = getDataStackedColumn("FIRSTNAME", 5, 9);

    assertEquals(firstListStackedKey(listIndexed), "Ivanov");
    assertEquals(firstListStackedValue(listIndexed), 2);

    assertEquals(firstListStackedKey(listNotIndexed), "Lee");
    assertEquals(firstListStackedValue(listNotIndexed), 2);

    assertEquals(lastListStackedKey(listIndexed), "Шаляпин");
    assertEquals(lastListStackedKey(listNotIndexed), "Федор");
  }

  @Test
  public void computeBeginEnd29Test() throws BeginEndWrongOrderException, SqlColMetadataException {
    List<StackedColumn> listIndexed = getDataStackedColumn("LASTNAME", 2, 9);
    List<StackedColumn> listNotIndexed = getDataStackedColumn("FIRSTNAME", 2, 9);

    assertEquals(firstListStackedKey(listIndexed), "Ivanov");
    assertEquals(firstListStackedValue(listIndexed), 3);

    assertEquals(firstListStackedKey(listNotIndexed), "Ivan");
    assertEquals(firstListStackedValue(listNotIndexed), 1);

    assertEquals(lastListStackedKey(listIndexed), "Шаляпин");
    assertEquals(lastListStackedKey(listNotIndexed), "Федор");
  }

  @Test
  public void computeBeginEnd10Test() throws BeginEndWrongOrderException, SqlColMetadataException {
    List<StackedColumn> lastName = getDataStackedColumn("LASTNAME", 0, 10);
    List<StackedColumn> firstName = getDataStackedColumn("FIRSTNAME", 0, 10);
    List<StackedColumn> firstNameFilter = getDataStackedColumnFilter("FIRSTNAME", "LASTNAME", "Petrov", 0, 10);

    assertEquals(firstListStackedKey(lastName), "Ivanov");
    assertEquals(firstListStackedValue(lastName), 4);

    assertEquals(firstListStackedKey(firstName), "Alex");
    assertEquals(firstListStackedValue(firstName), 1);

    assertEquals(firstListStackedKey(firstNameFilter), "Oleg");
    assertEquals(firstListStackedValue(firstNameFilter), 1);

    assertEquals(lastListStackedKey(lastName), "Пирогов");
    assertEquals(lastListStackedKey(firstName), "Петр");
    assertEquals(lastListStackedKey(firstNameFilter), "Men");
  }

  @Test
  public void computeBeginEnd11Test() throws BeginEndWrongOrderException, SqlColMetadataException {
    List<StackedColumn> listIndexed = getDataStackedColumn("LASTNAME", 11, 11);
    List<StackedColumn> listNotIndexed = getDataStackedColumn("FIRSTNAME", 11, 11);

    assertEquals(firstListStackedKey(listIndexed), "Semenov");
    assertEquals(firstListStackedValue(listIndexed), 1);

    assertEquals(firstListStackedKey(listNotIndexed), "Oleg");
    assertEquals(firstListStackedValue(listNotIndexed), 1);
  }

  @Test
  public void computeBeginEnd2527Test() throws BeginEndWrongOrderException, SqlColMetadataException {
    List<StackedColumn> listIndexed = getDataStackedColumn("LASTNAME", 25, 27);
    List<StackedColumn> listNotIndexed = getDataStackedColumn("FIRSTNAME", 25, 27);

    assertEquals(firstListStackedKey(listIndexed), "Semenov");
    assertEquals(firstListStackedValue(listIndexed), 1);

    assertEquals(firstListStackedKey(listNotIndexed), "Egor");
    assertEquals(firstListStackedValue(listNotIndexed), 1);

    assertEquals(lastListStackedKey(listIndexed), "Ivanov");
    assertEquals(lastListStackedKey(listNotIndexed), "Ivan");
  }

}
