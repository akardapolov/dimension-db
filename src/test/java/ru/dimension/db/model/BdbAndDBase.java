package ru.dimension.db.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import ru.dimension.db.backend.BerkleyDB;
import ru.dimension.db.core.DStore;

@Data
@Builder(toBuilder = true)
@AllArgsConstructor
public class BdbAndDBase {
  private DStore dStore;
  private BerkleyDB berkleyDB;
}
