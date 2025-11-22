package ru.dimension.db.core;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.dbcp2.BasicDataSource;
import ru.dimension.db.config.DBaseConfig;
import ru.dimension.db.service.impl.GroupByServiceImpl;
import ru.dimension.db.service.impl.RawServiceImpl;
import ru.dimension.db.service.impl.StoreServiceImpl;
import ru.dimension.db.storage.Converter;
import ru.dimension.db.storage.firebird.DimensionFirebirdImpl;
import ru.dimension.db.storage.firebird.EnumFirebirdImpl;
import ru.dimension.db.storage.firebird.HistogramFirebirdImpl;
import ru.dimension.db.storage.firebird.RawFirebirdImpl;

@Log4j2
public class FirebirdStore extends CommonStore implements DStore {

  private final BasicDataSource basicDataSource;

  public FirebirdStore(DBaseConfig dBaseConfig, BasicDataSource basicDataSource) {
    super(dBaseConfig);

    this.basicDataSource = basicDataSource;

    this.rawDAO = new RawFirebirdImpl(this.metaModelApi, this.basicDataSource);
    this.enumDAO = new EnumFirebirdImpl(this.basicDataSource);
    this.histogramDAO = new HistogramFirebirdImpl(this.basicDataSource);
    this.dimensionDAO = new DimensionFirebirdImpl(this.basicDataSource);

    this.converter = new Converter(dimensionDAO);

    this.rawService = new RawServiceImpl(this.metaModelApi, converter, rawDAO, histogramDAO, enumDAO);
    this.groupByService = new GroupByServiceImpl(this.metaModelApi, converter, histogramDAO, rawDAO, enumDAO);
    this.storeService = new StoreServiceImpl(this.metaModelApi, this.statisticsService, converter, rawDAO, enumDAO, histogramDAO);
  }
}