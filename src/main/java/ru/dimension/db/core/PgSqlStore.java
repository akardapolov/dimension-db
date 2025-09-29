package ru.dimension.db.core;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.dbcp2.BasicDataSource;
import ru.dimension.db.config.DBaseConfig;
import ru.dimension.db.service.impl.GroupByServiceImpl;
import ru.dimension.db.service.impl.RawServiceImpl;
import ru.dimension.db.service.impl.StoreServiceImpl;
import ru.dimension.db.storage.Converter;
import ru.dimension.db.storage.pqsql.DimensionPgSqlImpl;
import ru.dimension.db.storage.pqsql.EnumPgSqlImpl;
import ru.dimension.db.storage.pqsql.HistogramPgSqlImpl;
import ru.dimension.db.storage.pqsql.RawPgSqlImpl;

@Log4j2
public class PgSqlStore extends CommonStore implements DStore {

  private final BasicDataSource basicDataSource;

  public PgSqlStore(DBaseConfig dBaseConfig,
                    BasicDataSource basicDataSource) {
    super(dBaseConfig);

    this.basicDataSource = basicDataSource;

    this.rawDAO = new RawPgSqlImpl(this.metaModelApi, this.basicDataSource);
    this.enumDAO = new EnumPgSqlImpl(this.basicDataSource);
    this.histogramDAO = new HistogramPgSqlImpl(this.basicDataSource);
    this.dimensionDAO = new DimensionPgSqlImpl(this.basicDataSource);

    this.converter = new Converter(dimensionDAO);

    this.rawService = new RawServiceImpl(this.metaModelApi, converter, rawDAO, histogramDAO, enumDAO);
    this.groupByService = new GroupByServiceImpl(this.metaModelApi, converter, histogramDAO, rawDAO, enumDAO);
    this.storeService = new StoreServiceImpl(this.metaModelApi, this.statisticsService, converter, rawDAO, enumDAO, histogramDAO);
  }
}
