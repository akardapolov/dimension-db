package ru.dimension.db.storage;

import static ru.dimension.db.util.MapArrayUtil.arrayToString;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Wrapper;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.extern.log4j.Log4j2;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.service.mapping.Mapper;
import ru.dimension.db.storage.helper.ClickHouseHelper;

@Log4j2
public class Converter {

  private final DimensionDAO dimensionDAO;

  public Converter(DimensionDAO dimensionDAO) {
    this.dimensionDAO = dimensionDAO;
  }

  public int convertRawToInt(Object obj,
                             CProfile cProfile) {
    if (obj == null) {
      return Mapper.INT_NULL;
    }

    switch (cProfile.getCsType().getDType()) {
      case DATE:
        if (obj instanceof java.util.Date dt) {
          return dimensionDAO.getOrLoad(dt.toString());
        } else if (obj instanceof LocalDateTime localDateTime) {
          return dimensionDAO.getOrLoad(localDateTime.toString());
        } else if (obj instanceof LocalDate localDate) {
          return dimensionDAO.getOrLoad(localDate.toString());
        }
      case TIMESTAMP:
      case TIMESTAMPTZ:
      case DATETIME:
      case DATETIME2:
      case SMALLDATETIME:
        if (obj instanceof Timestamp ts) {
          return Math.toIntExact(ts.getTime() / 1000);
        } else if (obj instanceof LocalDateTime localDateTime) {
          return Math.toIntExact(localDateTime.atZone(ZoneOffset.UTC).toInstant().toEpochMilli() / 1000);
        }
      case OID:
      case UINT32:
      case UINT64:
      case SERIAL:
      case SMALLSERIAL:
      case BIGSERIAL:
      case LONG:
        if (ClickHouseHelper.checkUnsigned(obj.getClass().getName())) {
          return ClickHouseHelper.invokeMethod(obj, "intValue", Integer.class);
        } else {
          Long l = (Long) obj;
          return l.intValue();
        }
      case UINT8:
      case UINT16:
      case INT16:
      case INT32:
      case INT64:
      case INT128:
      case INT256:
      case INT2:
      case INT4:
      case INT8:
      case NUMBER:
      case INTEGER:
      case BIGINT:
      case SMALLINT:
      case INT:
      case TIME:
      case TIMETZ:
      case TINYINT:
        if (obj instanceof BigDecimal bd) {
          return bd.intValue();
        } else if (obj instanceof Double db) {
          return db.intValue();
        } else if (obj instanceof Long lng) {
          return lng.intValue();
        } else if (obj instanceof Short sh) {
          return sh.intValue();
        } else if (obj instanceof Time t) {
          return Math.toIntExact(t.getTime());
        } else if (obj instanceof Float f) {
          return f.intValue();
        } else if (obj instanceof Byte b) {
          return b.intValue();
        } else if (ClickHouseHelper.checkUnsigned(obj.getClass().getName())) {
          return ClickHouseHelper.invokeMethod(obj, "intValue", Integer.class);
        }
        return (Integer) obj;
      case BIT:
        if (obj instanceof Boolean b) {
          return b ? 1 : 0;
        }
      case FLOAT64:
      case DOUBLE:
      case DECIMAL:
      case FLOAT:
      case NUMERIC:
      case MONEY:
      case SMALLMONEY:
        if (obj instanceof BigDecimal bd) {
          return dimensionDAO.getOrLoad(bd.doubleValue());
        } else {
          Double varD = (Double) obj;
          return dimensionDAO.getOrLoad(varD);
        }
      case FLOAT8:
        Double d = (Double) obj;
        return dimensionDAO.getOrLoad(d);
      case FLOAT4:
      case REAL:
      case FLOAT32:
        Float f = (Float) obj;
        return dimensionDAO.getOrLoad(f.doubleValue());
      case ENUM8:
      case ENUM16:
      case FIXEDSTRING:
      case CHAR:
      case SYSNAME:
      case NCLOB:
      case NCHAR:
      case BPCHAR:
      case CLOB:
      case NAME:
      case TEXT:
      case NTEXT:
      case VARCHAR:
      case NVARCHAR2:
      case VARCHAR2:
      case NVARCHAR:
      case NULLABLE:
      case SET:
        return dimensionDAO.getOrLoad((String) obj);
      case ARRAY:
        if (obj.getClass().isArray()) {
          return dimensionDAO.getOrLoad(arrayToString(obj));
        }
      case MAP:
        if (obj instanceof LinkedHashMap map) {
          return dimensionDAO.getOrLoad(String.valueOf(map));
        } else {
          Map<?, ?> mapValue = (Map<?, ?>) obj;
          return dimensionDAO.getOrLoad(String.valueOf(mapValue));
        }
      case IPV4:
        Inet4Address inet4Address = (Inet4Address) obj;
        return dimensionDAO.getOrLoad(inet4Address.getHostAddress());
      case IPV6:
        Inet6Address inet6Address = (Inet6Address) obj;
        return dimensionDAO.getOrLoad(inet6Address.getHostAddress());
      case BYTEA:
      case JSONB:
      case POINT:
      case BINARY:
      case RAW:
      case VARBINARY:
        return dimensionDAO.getOrLoad(new String((byte[]) obj, StandardCharsets.UTF_8));
      default:
        return Mapper.INT_NULL;
    }
  }

  public String convertIntToRaw(int objIndex,
                                CProfile cProfile) {
    if (objIndex == Mapper.INT_NULL) return "";

    if (cProfile.getColDbTypeName().contains("ENUM")) return dimensionDAO.getStringById(objIndex);
    if (cProfile.getColDbTypeName().contains("FIXEDSTRING")) return dimensionDAO.getStringById(objIndex);
    if (cProfile.getColDbTypeName().contains("ARRAY")) return dimensionDAO.getStringById(objIndex);
    if (cProfile.getColDbTypeName().contains("MAP")) return dimensionDAO.getStringById(objIndex);
    if (cProfile.getColDbTypeName().contains("SET")) return dimensionDAO.getStringById(objIndex);

    return switch (Mapper.isDBType(cProfile)) {
      case DATE, ENUM8, ENUM16, CHAR, NCHAR, NCLOB, CLOB, NAME, TEXT, NTEXT,
          VARCHAR, NVARCHAR2, VARCHAR2, NVARCHAR, RAW, VARBINARY, BYTEA, JSONB, POINT, INTERVAL, BINARY, SYSNAME, NULLABLE, STRING ->
          dimensionDAO.getStringById(objIndex);
      case IPV4, IPV6 -> getCanonicalHost(dimensionDAO.getStringById(objIndex));
      case TIMESTAMP, TIMESTAMPTZ, DATETIME, DATETIME2, SMALLDATETIME -> getDateForLongShorted(objIndex);
      case FLOAT64, DECIMAL, FLOAT4, REAL, FLOAT8, FLOAT32, FLOAT, NUMERIC, MONEY, SMALLMONEY, DOUBLE ->
          String.valueOf(dimensionDAO.getDoubleById(objIndex));
      default -> String.valueOf(objIndex);
    };
  }

  public double convertIntToDouble(int objIndex,
                                   CProfile cProfile) {
    return switch (cProfile.getCsType().getDType()) {
      case FLOAT64, DECIMAL, FLOAT4, REAL, FLOAT8, FLOAT32, FLOAT, NUMERIC, MONEY, SMALLMONEY, DOUBLE ->
          dimensionDAO.getDoubleById(objIndex);
      default -> objIndex;
    };
  }

  public long getKeyValue(Object obj, CProfile cProfile) {
    if (obj == null) {
      return 0L;
    }

    switch (cProfile.getCsType().getDType()) {
      case LONG, INTEGER -> {
        if (obj instanceof Long l) {
          return l;
        } else {
          return ((Integer) obj).longValue();
        }
      }
      case DATE, TIMESTAMP, TIMESTAMPTZ, DATETIME, DATETIME2, SMALLDATETIME -> {
        if (obj instanceof Timestamp ts) {
          return ts.getTime();
        } else if (obj instanceof Instant instant) {
          return instant.toEpochMilli();
        } else if (obj instanceof LocalDateTime localDateTime) {
          ZonedDateTime zdt = localDateTime.atZone(ZoneId.systemDefault());
          return zdt.toInstant().toEpochMilli();
        } else if (obj instanceof LocalDate localDate) {
          return localDate.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        } else if (obj instanceof OffsetDateTime offsetDateTime) {
          return offsetDateTime.toInstant().toEpochMilli();
        } else if (obj instanceof ZonedDateTime zonedDateTime) {
          return zonedDateTime.toInstant().toEpochMilli();
        } else if (obj instanceof Date date) {
          return date.getTime();
        } else {
          return handleOracleTimestamp(obj);
        }
      }
      default -> {
        return 0L;
      }
    }
  }

  private long handleOracleTimestamp(Object obj) {
    if (obj instanceof Timestamp) {
      return ((Timestamp) obj).getTime();
    }
    if (obj instanceof Wrapper) {
      try {
        Timestamp ts = ((Wrapper) obj).unwrap(Timestamp.class);
        return ts.getTime();
      } catch (SQLException ignored) {
        log.warn("Unwrap failed, fall through");
      }
    }
    return 0L;
  }

  private String getDateForLongShorted(int longDate) {
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
    Date dtDate = new Date(((long) longDate) * 1000L);
    return simpleDateFormat.format(dtDate);
  }

  private String getCanonicalHost(String host) {
    try {
      return InetAddress.getByName(host).getHostAddress();
    } catch (UnknownHostException e) {
      log.info(e.getMessage());
      return host;
    }
  }
}
