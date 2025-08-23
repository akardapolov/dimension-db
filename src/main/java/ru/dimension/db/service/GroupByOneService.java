package ru.dimension.db.service;

import java.util.List;
import ru.dimension.db.exception.SqlColMetadataException;
import ru.dimension.db.model.GroupFunction;
import ru.dimension.db.model.filter.CompositeFilter;
import ru.dimension.db.model.output.StackedColumn;
import ru.dimension.db.model.profile.CProfile;

public interface GroupByOneService {

  List<StackedColumn> getStacked(String tableName,
                                 CProfile cProfile,
                                 GroupFunction groupFunction,
                                 CompositeFilter compositeFilter,
                                 long begin,
                                 long end)
      throws SqlColMetadataException;
}
