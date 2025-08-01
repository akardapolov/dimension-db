package ru.dimension.db.storage.bdb.entity.dictionary;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class DIntDouble {

  @PrimaryKey(sequence = "ParamDIntDoubleSeq")
  private int param;

  @SecondaryKey(relate = Relationship.ONE_TO_ONE)
  private double value;
}
