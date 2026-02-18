package ru.dimension.db.core.metamodel;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.log4j.Log4j2;
import ru.dimension.db.model.MetaModel;
import ru.dimension.db.model.MetaModel.TableMetadata;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.table.AType;
import ru.dimension.db.model.profile.table.BType;
import ru.dimension.db.model.profile.table.IType;
import ru.dimension.db.model.profile.table.TType;

@Log4j2
public class MetaModelApiImpl implements MetaModelApi {

  private final MetaModel metaModel;

  public MetaModelApiImpl(MetaModel metaModel) {
    this.metaModel = metaModel;
  }

  @Override
  public byte getTableId(String tableName) {
    TableMetadata tableMetadata = metaModel.getMetadata().get(tableName);
    if (tableMetadata == null) {
      throw new RuntimeException("Table not found: " + tableName);
    }
    return tableMetadata.getTableId();
  }

  @Override
  public String getTableName(byte tableId) {
    for (Map.Entry<String, TableMetadata> entry : metaModel.getMetadata().entrySet()) {
      if (entry.getValue().getTableId() == tableId) {
        return entry.getKey();
      }
    }
    throw new RuntimeException("Table not found for id: " + tableId);
  }

  @Override
  public List<CProfile> getCProfiles(String tableName) {
    TableMetadata tableMetadata = metaModel.getMetadata().get(tableName);
    if (tableMetadata == null) {
      throw new RuntimeException("Table not found: " + tableName);
    }
    return tableMetadata.getCProfiles();
  }

  @Override
  public CProfile getTimestampCProfile(String tableName) {
    TableMetadata tableMetadata = metaModel.getMetadata().get(tableName);
    if (tableMetadata == null) {
      throw new RuntimeException("Table not found: " + tableName);
    }

    Optional<CProfile> tsCProfile = tableMetadata.getCProfiles().stream()
        .filter(cp -> cp.getCsType().isTimeStamp())
        .findAny();

    return tsCProfile.orElse(null);
  }

  @Override
  public BType getBackendType(String tableName) {
    TableMetadata tableMetadata = metaModel.getMetadata().get(tableName);
    if (tableMetadata == null) {
      throw new RuntimeException("Table not found: " + tableName);
    }
    return tableMetadata.getBackendType();
  }

  @Override
  public TType getTableType(String tableName) {
    TableMetadata tableMetadata = metaModel.getMetadata().get(tableName);
    if (tableMetadata == null) {
      throw new RuntimeException("Table not found: " + tableName);
    }
    return tableMetadata.getTableType();
  }

  @Override
  public IType getIndexType(String tableName) {
    return metaModel.getMetadata().get(tableName).getIndexType();
  }

  @Override
  public AType getAnalyzeType(String tableName) {
    return metaModel.getMetadata().get(tableName).getAnalyzeType();
  }

  @Override
  public Boolean getTableCompression(String tableName) {
    return metaModel.getMetadata().get(tableName).getCompression();
  }
}