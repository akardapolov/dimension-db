package ru.dimension.db.model;

import static com.querydsl.core.types.PathMetadataFactory.forVariable;

import com.querydsl.core.types.Path;
import com.querydsl.core.types.PathMetadata;
import com.querydsl.core.types.dsl.*;

import javax.annotation.processing.Generated;
import java.time.LocalDateTime;

@Generated("com.querydsl.codegen.EntitySerializer")
public class QManager extends EntityPathBase<Manager> {
  private static final long serialVersionUID = -479242270L;

  public static final QManager manager = new QManager("manager");

  public final NumberPath<Integer> id = createNumber("id", Integer.class);
  public final StringPath firstname = createString("firstname");
  public final StringPath lastname = createString("lastname");
  public final NumberPath<Integer> house = createNumber("house", Integer.class);
  public final StringPath city = createString("city");
  public final TimePath<LocalDateTime> birthday = createTime("birthday", LocalDateTime.class);
  public final NumberPath<Double> salary = createNumber("salary", Double.class);
  public final NumberPath<Float> kpi = createNumber("kpi", Float.class);

  public QManager(String variable) {
    super(Manager.class, forVariable(variable));
  }

  public QManager(Path<? extends Manager> path) {
    super(path.getType(), path.getMetadata());
  }

  public QManager(PathMetadata metadata) {
    super(Manager.class, metadata);
  }
}