package ru.dimension.db.model.profile;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import ru.dimension.db.model.profile.cstype.CSType;
import ru.dimension.db.model.profile.table.AType;
import ru.dimension.db.model.profile.table.BType;
import ru.dimension.db.model.profile.table.IType;
import ru.dimension.db.model.profile.table.TType;

@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
@Data
@Builder(toBuilder = true)
public class SProfile {

  private String tableName;
  private TType tableType = TType.TIME_SERIES;
  private IType indexType = IType.GLOBAL;
  private AType analyzeType = AType.ON_LOAD;
  private BType backendType = BType.BERKLEYDB;
  private Boolean compression = Boolean.FALSE;
  private Map<String, CSType> csTypeMap;
}
