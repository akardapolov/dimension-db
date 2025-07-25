package ru.dimension.db.storage.bdb.entity.column;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.dimension.db.metadata.CompressType;
import ru.dimension.db.storage.bdb.entity.ColumnKey;

@Entity
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class RColumn {

  @PrimaryKey
  private ColumnKey columnKey;

  private CompressType compressionType = CompressType.NONE;

  private byte[] dataByte;

  private int[] dataInt;

  private long[] dataLong;

  private float[] dataFloat;

  private double[] dataDouble;

  private String[] dataString;
}
