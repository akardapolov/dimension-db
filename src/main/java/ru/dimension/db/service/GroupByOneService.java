package ru.dimension.db.service;

import java.util.List;
import ru.dimension.db.exception.BeginEndWrongOrderException;
import ru.dimension.db.exception.SqlColMetadataException;
import ru.dimension.db.model.CompareFunction;
import ru.dimension.db.model.GroupFunction;
import ru.dimension.db.model.output.StackedColumn;
import ru.dimension.db.model.profile.CProfile;

public interface GroupByOneService {

  List<StackedColumn> getListStackedColumn(String tableName,
                                           CProfile cProfile,
                                           GroupFunction groupFunction,
                                           long begin,
                                           long end)
      throws SqlColMetadataException;

  List<StackedColumn> getListStackedColumnFilter(String tableName,
                                                 CProfile cProfile,
                                                 GroupFunction groupFunction,
                                                 CProfile cProfileFilter,
                                                 String[] filterData,
                                                 CompareFunction compareFunction,
                                                 long begin,
                                                 long end)
      throws SqlColMetadataException, BeginEndWrongOrderException;
}
