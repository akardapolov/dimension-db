package ru.dimension.db.handler;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.log4j.Log4j2;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.SProfile;
import ru.dimension.db.model.profile.cstype.CSType;

@Log4j2
public class MetadataHandler {

  public static List<CProfile> getDirectCProfileList(SProfile sProfile) {
    List<CProfile> cProfileList = new ArrayList<>();

    AtomicInteger counter = new AtomicInteger(0);

    sProfile.getCsTypeMap().forEach((columName, csType) ->
        cProfileList.add(CProfile.builder()
            .colId(counter.getAndAdd(1))
            .colIdSql(counter.get())
            .colDbTypeName(csType.getCType().name().toUpperCase())
            .colName(columName)
            .csType(CSType.builder()
                .isTimeStamp(csType.isTimeStamp())
                .sType(csType.getSType())
                .cType(csType.getCType())
                .dType(csType.getDType())
                .build())
            .build()));

    return cProfileList;
  }

  public static List<CProfile> getJdbcCProfileList(Connection connection,
                                                   String select) throws SQLException {
    List<CProfile> cProfileList = new ArrayList<>();

    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(select)) { // Try-with-resources for automatic closing

      ResultSetMetaData metaData = resultSet.getMetaData();
      int columnCount = metaData.getColumnCount();

      for (int i = 1; i <= columnCount; i++) {
        cProfileList.add(i - 1,
                         CProfile.builder()
                             .colId(i - 1)
                             .colIdSql(i)
                             .colName(metaData.getColumnName(i).toUpperCase())
                             .colDbTypeName(metaData.getColumnTypeName(i).toUpperCase())
                             .colSizeDisplay(metaData.getColumnDisplaySize(i))
                             .build());
      }
    }

    return cProfileList;
  }

  public static List<CProfile> getJdbcCProfileList(Connection connection, String sqlSchemaName, String sqlTableName) throws SQLException {
    List<CProfile> cProfileList = new ArrayList<>();

    String query = """
        SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH 
        FROM ALL_TAB_COLUMNS 
        WHERE OWNER = ? AND TABLE_NAME = ?
        """;

    try (PreparedStatement statement = connection.prepareStatement(query)) {
      statement.setString(1, sqlSchemaName.toUpperCase());
      statement.setString(2, sqlTableName.toUpperCase());
      try (ResultSet resultSet = statement.executeQuery()) {
        int colId = 0;
        while (resultSet.next()) {
          cProfileList.add(CProfile.builder()
                               .colId(colId++)
                               .colIdSql(colId)
                               .colName(resultSet.getString("COLUMN_NAME").toUpperCase())
                               .colDbTypeName(resultSet.getString("DATA_TYPE").toUpperCase())
                               .colSizeDisplay(resultSet.getInt("DATA_LENGTH"))
                               .build());
        }
      }
    }
    return cProfileList;
  }
}
