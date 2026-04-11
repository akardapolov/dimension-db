package ru.dimension.db.storage.dialect;

import ru.dimension.db.model.profile.CProfile;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class SQLiteDialect extends GenericDialect {

  private static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
  private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

  @Override
  public void setDateTime(CProfile cProfile,
                          PreparedStatement ps,
                          int index,
                          long unixTimestamp) throws SQLException {
    String typeName = cProfile.getColDbTypeName().toUpperCase();

    if (typeName.contains("DATETIME")
        || typeName.contains("TEXT")
        || typeName.contains("VARCHAR")
        || typeName.contains("STRING")) {
      SimpleDateFormat sdf = new SimpleDateFormat(DATETIME_FORMAT);
      sdf.setTimeZone(UTC);
      ps.setString(index, sdf.format(new Date(unixTimestamp)));
    } else {
      ps.setLong(index, unixTimestamp);
    }
  }
}