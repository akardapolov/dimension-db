package ru.dimension.db.service;

import java.util.List;
import ru.dimension.db.exception.BeginEndWrongOrderException;
import ru.dimension.db.exception.SqlColMetadataException;
import ru.dimension.db.model.OrderBy;
import ru.dimension.db.model.filter.CompositeFilter;
import ru.dimension.db.model.output.GanttColumnCount;
import ru.dimension.db.model.output.GanttColumnSum;
import ru.dimension.db.model.profile.CProfile;

public interface GroupByService {

  List<GanttColumnCount> getGanttCount(String tableName,
                                       CProfile firstGrpBy,
                                       CProfile secondGrpBy,
                                       CompositeFilter compositeFilter,
                                       long begin,
                                       long end) throws SqlColMetadataException;

  List<GanttColumnCount> getGanttCount(String tableName,
                                       CProfile firstGrpBy,
                                       CProfile secondGrpBy,
                                       CompositeFilter compositeFilter,
                                       int batchSize,
                                       long begin,
                                       long end) throws SqlColMetadataException, BeginEndWrongOrderException;

  List<GanttColumnSum> getGanttSum(String tableName,
                                   CProfile firstGrpBy,
                                   CProfile secondGrpBy,
                                   CompositeFilter compositeFilter,
                                   long begin,
                                   long end) throws SqlColMetadataException;

  List<String> getDistinct(String tableName,
                           CProfile cProfile,
                           OrderBy orderBy,
                           CompositeFilter compositeFilter,
                           int limit,
                           long begin,
                           long end);
}
