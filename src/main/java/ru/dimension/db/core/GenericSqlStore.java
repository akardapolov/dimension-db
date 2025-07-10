package ru.dimension.db.core;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.dbcp2.BasicDataSource;
import ru.dimension.db.config.DBaseConfig;
import ru.dimension.db.service.impl.EnumServiceImpl;
import ru.dimension.db.service.impl.GroupByOneServiceImpl;
import ru.dimension.db.service.impl.GroupByServiceImpl;
import ru.dimension.db.service.impl.HistogramServiceImpl;
import ru.dimension.db.service.impl.RawServiceImpl;
import ru.dimension.db.service.impl.StoreServiceImpl;
import ru.dimension.db.storage.Converter;
import ru.dimension.db.storage.dialect.DatabaseDialect;
import ru.dimension.db.storage.jdbc.DimensionJdbcImpl;
import ru.dimension.db.storage.jdbc.EnumJdbcImpl;
import ru.dimension.db.storage.jdbc.HistogramJdbcImpl;
import ru.dimension.db.storage.jdbc.RawJdbcImpl;

@Log4j2
public class GenericSqlStore extends CommonStore implements DStore {

  private final BasicDataSource basicDataSource;

  public GenericSqlStore(DBaseConfig dBaseConfig,
                         BasicDataSource basicDataSource,
                         DatabaseDialect databaseDialect) {
    super(dBaseConfig);

    this.basicDataSource = basicDataSource;

    this.rawDAO = new RawJdbcImpl(this.metaModelApi, this.basicDataSource, databaseDialect);
    this.enumDAO = new EnumJdbcImpl(this.basicDataSource);
    this.histogramDAO = new HistogramJdbcImpl(this.basicDataSource);
    this.dimensionDAO = new DimensionJdbcImpl(this.basicDataSource);

    this.converter = new Converter(dimensionDAO);

    this.histogramsService = new HistogramServiceImpl(this.metaModelApi, converter, histogramDAO, rawDAO);
    this.rawService = new RawServiceImpl(this.metaModelApi, converter, rawDAO, histogramDAO, enumDAO);
    this.enumService = new EnumServiceImpl(this.metaModelApi, converter, rawDAO, enumDAO);
    this.groupByService = new GroupByServiceImpl(this.metaModelApi, converter, histogramDAO, rawDAO, enumDAO);
    this.groupByOneService = new GroupByOneServiceImpl(this.metaModelApi, converter, histogramDAO, rawDAO, enumDAO);

    this.storeService = new StoreServiceImpl(this.metaModelApi, this.statisticsService, converter, rawDAO, enumDAO, histogramDAO);
  }
}
