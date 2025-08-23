package ru.dimension.db.storage.bdb.entity;

import com.sleepycat.persist.model.KeyField;
import com.sleepycat.persist.model.Persistent;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Persistent
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
@EqualsAndHashCode
public class ColumnKey {

  @KeyField(1)
  private byte tableId;

  @KeyField(2)
  private long blockId;

  @KeyField(3)
  private int colId;
}
