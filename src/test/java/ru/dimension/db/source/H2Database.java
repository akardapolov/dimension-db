package ru.dimension.db.source;

import com.querydsl.core.types.QBean;
import com.querydsl.core.types.dsl.EntityPathBase;
import com.querydsl.sql.HSQLDBTemplates;
import com.querydsl.sql.RelationalPathBase;
import com.querydsl.sql.SQLTemplates;
import com.querydsl.sql.dml.SQLInsertClause;
import com.querydsl.sql.mysql.MySQLQuery;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import ru.dimension.db.core.DStore;
import ru.dimension.db.exception.EnumByteExceedException;
import ru.dimension.db.exception.SqlColMetadataException;
import ru.dimension.db.model.Manager;
import ru.dimension.db.model.Person;
import ru.dimension.db.model.QManager;
import ru.dimension.db.model.QPerson;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.TProfile;

public class H2Database {

  @Getter
  private final Connection connection;

  @Getter
  private final List<CProfile> cProfileList = new ArrayList<>();

  public H2Database(String url) throws SQLException {
    connection = DriverManager.getConnection(url);
  }

  public void execute(String command) throws SQLException {
    Statement statement = connection.createStatement();
    statement.execute(command);
    statement.close();
  }

  public <T> List<T> getAll(EntityPathBase<T> entity, QBean<T> projections) {

    SQLTemplates dialect = new HSQLDBTemplates(); // SQL-dialect

    MySQLQuery query = new MySQLQuery(connection, dialect);

    List<T> list = query.select(projections)
        .from(entity)
        .fetch();

    return list;
  }

  public void insert(Person p) {

    QPerson qp = new QPerson("person");

    SQLTemplates dialect = new HSQLDBTemplates(); // SQL-dialect

    RelationalPathBase relationalPathBase=
        new RelationalPathBase(qp.getType(), qp.getMetadata(), "","person");

    new SQLInsertClause(connection, dialect, relationalPathBase)
        .columns(qp.id, qp.firstname, qp.lastname, qp.house, qp.city, qp.birthday)
        .values(p.getId(), p.getFirstname(), p.getLastname(), p.getHouse(), p.getCity(), p.getBirthday()).execute();
  }

  public void insert(Manager manager) {
    QManager qm = new QManager("manager");

    SQLTemplates dialect = new HSQLDBTemplates(); // SQL-dialect

    RelationalPathBase relationalPathBase =
        new RelationalPathBase(qm.getType(), qm.getMetadata(), "", "manager");

    new SQLInsertClause(connection, dialect, relationalPathBase)
        .columns(qm.id, qm.firstname, qm.lastname, qm.house, qm.city, qm.birthday, qm.salary, qm.kpi)
        .values(manager.getId(), manager.getFirstname(), manager.getLastname(),
                manager.getHouse(), manager.getCity(), manager.getBirthday(),
                manager.getSalary(), manager.getKpi()).execute();
  }

  public List<List<Object>> getData(String select) throws SQLException {
    List<List<Object>> listsColStore = new ArrayList<>();

    cProfileList.forEach(v -> listsColStore.add(v.getColId(), new ArrayList<>()));

    PreparedStatement ps = connection.prepareStatement(select);
    ResultSet r = ps.executeQuery();

    while (r.next()) {
      cProfileList.forEach(v -> {
        try {
          addToList(listsColStore, v, r);
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      });
    }

    r.close();
    ps.close();

    return listsColStore;
  }

  public List<List<Object>> putDataJdbc(DStore dStore, TProfile tProfile, String select) throws SQLException {
    List<List<Object>> listsColStore = new ArrayList<>();

    cProfileList.forEach(v -> listsColStore.add(v.getColId(), new ArrayList<>()));

    PreparedStatement ps = connection.prepareStatement(select, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    ResultSet r = ps.executeQuery();

    try {
      dStore.putDataJdbc(tProfile.getTableName(), r);
    } catch (SqlColMetadataException | EnumByteExceedException e) {
      throw new RuntimeException(e);
    }

    r.close();
    ps.close();

    return listsColStore;
  }

  public List<List<Object>> putDataJdbcBatch(DStore dStore, TProfile tProfile, String select, Integer fBaseBatchSize) throws SQLException {
    List<List<Object>> listsColStore = new ArrayList<>();

    cProfileList.forEach(v -> listsColStore.add(v.getColId(), new ArrayList<>()));

    PreparedStatement ps = connection.prepareStatement(select);
    ResultSet r = ps.executeQuery();

    try {
      dStore.putDataJdbcBatch(tProfile.getTableName(), r, fBaseBatchSize);
    } catch (SqlColMetadataException | EnumByteExceedException e) {
      throw new RuntimeException(e);
    }

    r.close();
    ps.close();

    return listsColStore;
  }

  private void addToList(List<List<Object>> lists, CProfile v, ResultSet r ) throws SQLException {
    lists.get(v.getColId()).add(r.getObject(v.getColIdSql()));
  }

  public void loadSqlColMetadataList(String select) throws SQLException {
    Statement s;
    ResultSet rs;
    ResultSetMetaData rsmd;

    s = connection.createStatement();
    s.executeQuery(select);
    rs = s.getResultSet();
    rsmd = rs.getMetaData();

    for (int i = 1; i <= rsmd.getColumnCount(); i++) {
      cProfileList.add(i - 1,
      CProfile.builder()
          .colId(i-1)
          .colIdSql(i)
          .colName(rsmd.getColumnName(i).toUpperCase())
          .colDbTypeName(rsmd.getColumnTypeName(i).toUpperCase())
          .colSizeDisplay(rsmd.getColumnDisplaySize(i))
          .build());
    }

    rs.close();
    s.close();
  }

  public void close() throws SQLException {
    connection.close();
  }
}
