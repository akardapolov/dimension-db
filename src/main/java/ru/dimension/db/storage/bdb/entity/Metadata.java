package ru.dimension.db.storage.bdb.entity;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class Metadata {

  @PrimaryKey
  private MetadataKey metadataKey;

  private byte[] rawCTypeKeys;

  private int[] rawColIds;

  private int[] enumColIds;

  private int[] histogramColIds;
}
