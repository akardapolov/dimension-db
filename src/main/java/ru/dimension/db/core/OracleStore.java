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
import ru.dimension.db.storage.oracle.DimensionOracleImpl;
import ru.dimension.db.storage.oracle.EnumOracleImpl;
import ru.dimension.db.storage.oracle.HistogramOracleImpl;
import ru.dimension.db.storage.oracle.RawOracleImpl;

@Log4j2
public class OracleStore extends CommonStore implements DStore {

  private final BasicDataSource basicDataSource;

  public OracleStore(DBaseConfig dBaseConfig,
                     BasicDataSource basicDataSource) {
    super(dBaseConfig);

    this.basicDataSource = basicDataSource;

    this.rawDAO = new RawOracleImpl(this.metaModelApi, this.basicDataSource);
    this.enumDAO = new EnumOracleImpl(this.basicDataSource);
    this.histogramDAO = new HistogramOracleImpl(this.basicDataSource);
    this.dimensionDAO = new DimensionOracleImpl(this.basicDataSource);

    this.converter = new Converter(dimensionDAO);

    this.histogramsService = new HistogramServiceImpl(this.metaModelApi, converter, histogramDAO, rawDAO);
    this.rawService = new RawServiceImpl(this.metaModelApi, converter, rawDAO, histogramDAO, enumDAO);
    this.enumService = new EnumServiceImpl(this.metaModelApi, converter, rawDAO, enumDAO);
    this.groupByService = new GroupByServiceImpl(this.metaModelApi, converter, histogramDAO, rawDAO, enumDAO);
    this.groupByOneService = new GroupByOneServiceImpl(this.metaModelApi, converter, histogramDAO, rawDAO, enumDAO);

    this.storeService = new StoreServiceImpl(this.metaModelApi, this.statisticsService, converter, rawDAO, enumDAO, histogramDAO);
  }
}
