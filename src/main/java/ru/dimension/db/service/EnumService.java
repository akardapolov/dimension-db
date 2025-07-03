package ru.dimension.db.service;

import java.util.List;
import ru.dimension.db.exception.SqlColMetadataException;
import ru.dimension.db.model.output.StackedColumn;
import ru.dimension.db.model.profile.CProfile;

public interface EnumService {

  List<StackedColumn> getListStackedColumn(String tableName,
                                           CProfile cProfile,
                                           long begin,
                                           long end)
      throws SqlColMetadataException;
}
