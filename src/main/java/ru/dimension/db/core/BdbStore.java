package ru.dimension.db.core;

import com.sleepycat.persist.EntityStore;
import lombok.extern.log4j.Log4j2;
import ru.dimension.db.config.DBaseConfig;
import ru.dimension.db.service.impl.GroupByServiceImpl;
import ru.dimension.db.service.impl.RawServiceImpl;
import ru.dimension.db.service.impl.StoreServiceImpl;
import ru.dimension.db.storage.Converter;
import ru.dimension.db.storage.bdb.impl.DimensionBdbImpl;
import ru.dimension.db.storage.bdb.impl.EnumBdbImpl;
import ru.dimension.db.storage.bdb.impl.HistogramBdbImpl;
import ru.dimension.db.storage.bdb.impl.RawBdbImpl;

@Log4j2
public class BdbStore extends CommonStore implements DStore {

  public BdbStore(DBaseConfig dBaseConfig,
                  EntityStore store) {
    super(dBaseConfig, store);

    this.rawDAO = new RawBdbImpl(this.store);
    this.enumDAO = new EnumBdbImpl(this.store);
    this.histogramDAO = new HistogramBdbImpl(this.store);
    this.dimensionDAO = new DimensionBdbImpl(this.store);

    this.converter = new Converter(dimensionDAO);

    this.rawService = new RawServiceImpl(this.metaModelApi, converter, rawDAO, histogramDAO, enumDAO);
    this.groupByService = new GroupByServiceImpl(this.metaModelApi, converter, histogramDAO, rawDAO, enumDAO);
    this.storeService = new StoreServiceImpl(this.metaModelApi, this.statisticsService, converter, rawDAO, enumDAO, histogramDAO);
  }
}
