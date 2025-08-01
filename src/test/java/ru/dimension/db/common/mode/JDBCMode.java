package ru.dimension.db.common.mode;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import ru.dimension.db.core.DStore;
import ru.dimension.db.exception.EnumByteExceedException;
import ru.dimension.db.exception.SqlColMetadataException;
import ru.dimension.db.model.profile.SProfile;
import ru.dimension.db.model.profile.TProfile;
import ru.dimension.db.model.profile.table.AType;
import ru.dimension.db.model.profile.table.BType;
import ru.dimension.db.model.profile.table.IType;
import ru.dimension.db.model.profile.table.TType;

public interface JDBCMode {

  default SProfile getSProfileJdbc(
      String tableName,
      TType tableType,
      IType indexType,
      AType analyzeType,
      BType backendType,
      boolean compression
  ) {
    return SProfile.builder()
        .tableName(tableName)
        .tableType(tableType)
        .indexType(indexType)
        .analyzeType(analyzeType)
        .backendType(backendType)
        .compression(compression)
        .csTypeMap(new LinkedHashMap<>()).build();
  }

  default void putData(DStore dStore, TProfile tProfile, Connection connection, String select, boolean isBatch) throws SQLException {

    try (PreparedStatement ps = connection.prepareStatement(select, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY); ResultSet r = ps.executeQuery()) {

      if (isBatch) {
        dStore.putDataJdbcBatch(tProfile.getTableName(), r, 10);
      } else {
        dStore.putDataJdbc(tProfile.getTableName(), r);
      }
    } catch (SqlColMetadataException | EnumByteExceedException e) {
      throw new RuntimeException(e);
    }
  }

  default void putDataJdbc(DStore dStore, TProfile tProfile, Connection connection, String select) throws SQLException {
    putData(dStore, tProfile, connection, select, false);
  }

  default void putDataJdbcBatch(DStore dStore, TProfile tProfile, Connection connection, String select) throws SQLException {
    putData(dStore, tProfile, connection, select, true);
  }
}

