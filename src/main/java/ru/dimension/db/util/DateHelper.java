package ru.dimension.db.util;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class DateHelper {
  public static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss");

  private DateHelper() {}

  public static String format(LocalDateTime dateTime) {
    return dateTime.format(FORMATTER);
  }

}
