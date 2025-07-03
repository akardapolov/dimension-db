package ru.dimension.db.sql;

import java.util.List;

public interface BatchResultSet {

  List<List<Object>> getObject();

  boolean next();

}
