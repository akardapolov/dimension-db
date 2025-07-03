package ru.dimension.db.model.profile.cstype;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.dimension.db.metadata.DataType;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder(toBuilder = true)
public class CSType implements Serializable {

  private boolean isTimeStamp;
  private SType sType;
  private CType cType;
  private DataType dType;
}
