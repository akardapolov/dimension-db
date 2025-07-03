package ru.dimension.db.service;

import java.util.List;
import ru.dimension.db.exception.BeginEndWrongOrderException;
import ru.dimension.db.exception.GanttColumnNotSupportedException;
import ru.dimension.db.exception.SqlColMetadataException;
import ru.dimension.db.model.CompareFunction;
import ru.dimension.db.model.OrderBy;
import ru.dimension.db.model.output.GanttColumnCount;
import ru.dimension.db.model.output.GanttColumnSum;
import ru.dimension.db.model.profile.CProfile;

public interface GroupByService {

  List<GanttColumnCount> getListGanttColumn(String tableName,
                                            CProfile firstGrpBy,
                                            CProfile secondGrpBy,
                                            long begin,
                                            long end) throws SqlColMetadataException;

  List<GanttColumnCount> getListGanttColumn(String tableName,
                                            CProfile firstGrpBy,
                                            CProfile secondGrpBy,
                                            int batchSize,
                                            long begin,
                                            long end) throws SqlColMetadataException, BeginEndWrongOrderException;

  List<GanttColumnCount> getListGanttColumn(String tableName,
                                            CProfile firstGrpBy,
                                            CProfile secondGrpBy,
                                            CProfile cProfileFilter,
                                            String[] filterData,
                                            CompareFunction compareFunction,
                                            long begin,
                                            long end)
      throws SqlColMetadataException, BeginEndWrongOrderException, GanttColumnNotSupportedException;

  List<GanttColumnSum> getListGanttSumColumn(String tableName,
                                             CProfile firstGrpBy,
                                             CProfile secondGrpBy,
                                             long begin,
                                             long end) throws SqlColMetadataException;

  List<GanttColumnSum> getListGanttSumColumn(String tableName,
                                             CProfile firstGrpBy,
                                             CProfile secondGrpBy,
                                             CProfile cProfileFilter,
                                             String[] filterData,
                                             CompareFunction compareFunction,
                                             long begin,
                                             long end) throws SqlColMetadataException;

  List<String> getDistinct(String tableName,
                           CProfile cProfile,
                           OrderBy orderBy,
                           int limit,
                           long begin,
                           long end);

  List<String> getDistinct(String tableName,
                           CProfile cProfile,
                           OrderBy orderBy,
                           int limit,
                           long begin,
                           long end,
                           CProfile cProfileFilter,
                           String[] filterData,
                           CompareFunction compareFunction);
}
