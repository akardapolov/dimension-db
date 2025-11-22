package ru.dimension.db.storage;

import static ru.dimension.db.metadata.DataType.INT64;
import static ru.dimension.db.util.MapArrayUtil.arrayToString;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Wrapper;
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
import ru.dimension.db.util.DateHelper;

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
          return dimensionDAO.getOrLoad(dt.getTime());
        } else if (obj instanceof LocalDateTime localDateTime) {
          return dimensionDAO.getOrLoad(localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
        } else if (obj instanceof LocalDate localDate) {
          return dimensionDAO.getOrLoad(localDate.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli());
        }
        return dimensionDAO.getOrLoad(getKeyValue(obj, cProfile));
      case TIMESTAMP:
      case TIMESTAMPTZ:
      case DATETIME:
      case DATETIME2:
      case SMALLDATETIME:
        return dimensionDAO.getOrLoad(getKeyValue(obj, cProfile));
      case OID:
      case UINT32:
      case UINT64:
      case SERIAL:
      case SMALLSERIAL:
      case BIGSERIAL:
      case LONG:
        if (ClickHouseHelper.checkUnsigned(obj.getClass().getName())) {
          long longValue = ClickHouseHelper.invokeMethod(obj, "longValue", Long.class);
          return dimensionDAO.getOrLoad(longValue);
        } else {
          return dimensionDAO.getOrLoad(((Number) obj).longValue());
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
        if (cProfile.getCsType().getDType() == INT64) {
          return dimensionDAO.getOrLoad(((Number) obj).longValue());
        }
        if (obj instanceof BigDecimal bd) {
          return dimensionDAO.getOrLoad(bd.doubleValue());
        } else if (obj instanceof BigInteger bi) {
          return dimensionDAO.getOrLoad(bi.doubleValue());
        } else if (obj instanceof Double d) {
          return dimensionDAO.getOrLoad(d);
        } else if (obj instanceof Long l) {
          return dimensionDAO.getOrLoad(l);
        } else if (obj instanceof Short s) {
          return s.intValue();
        } else if (obj instanceof Time t) {
          return dimensionDAO.getOrLoad(t.getTime());
        } else if (obj instanceof Float f) {
          return dimensionDAO.getOrLoad(f);
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
        if (obj instanceof byte[]) {
          return dimensionDAO.getOrLoad(new String((byte[]) obj, StandardCharsets.UTF_8));
        } else if (obj instanceof Clob clob) {
          try {
            return dimensionDAO.getOrLoad(clob.getSubString(1, (int) clob.length()));
          } catch (SQLException e) {
            log.warn("Failed to read Clob in convertRawToInt: {}", e.getMessage());
            return dimensionDAO.getOrLoad(String.valueOf(obj));
          }
        } else if (obj instanceof Blob blob) {
          try {
            return dimensionDAO.getOrLoad(new String(blob.getBytes(1, (int) blob.length()), StandardCharsets.UTF_8));
          } catch (SQLException e) {
            log.warn("Failed to read Blob (Text) in convertRawToInt: {}", e.getMessage());
            return dimensionDAO.getOrLoad(String.valueOf(obj));
          }
        }
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
        if (obj instanceof byte[]) {
          return dimensionDAO.getOrLoad(new String((byte[]) obj, StandardCharsets.UTF_8));
        }
        return dimensionDAO.getOrLoad(String.valueOf(obj));
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
      case ENUM8, ENUM16, CHAR, NCHAR, NCLOB, CLOB, NAME, TEXT, NTEXT,
           VARCHAR, NVARCHAR2, VARCHAR2, NVARCHAR, RAW, VARBINARY, BYTEA, JSONB, POINT, INTERVAL, BINARY, SYSNAME, NULLABLE, STRING ->
          dimensionDAO.getStringById(objIndex);
      case IPV4, IPV6 -> getCanonicalHost(dimensionDAO.getStringById(objIndex));
      case UINT8, UINT16, INTEGER -> String.valueOf(objIndex);
      case DATE, TIMESTAMP, TIMESTAMPTZ, DATETIME, DATETIME2, SMALLDATETIME -> getDateForLong(objIndex);
      case LONG, OID, UINT32, UINT64, SERIAL, SMALLSERIAL, BIGSERIAL, BIGINT, INT64, INT128, INT256 ->
          String.valueOf(dimensionDAO.getLongById(objIndex));
      case FLOAT64, DECIMAL, FLOAT4, REAL, FLOAT8, FLOAT32, FLOAT, NUMERIC, MONEY, SMALLMONEY, DOUBLE ->
          String.valueOf(dimensionDAO.getDoubleById(objIndex));
      default -> {
        try {
          yield String.valueOf(dimensionDAO.getLongById(objIndex));
        } catch (Exception ed) {
          log.info(ed.getMessage());
          log.warn(cProfile);
          try {
            yield String.valueOf(dimensionDAO.getDoubleById(objIndex));
          } catch (Exception el) {
            log.info(el.getMessage());
            log.warn(cProfile);
            yield String.valueOf(objIndex);
          }
        }
      }
    };
  }

  public long convertIntToLong(int objIndex,
                               CProfile cProfile) {
    return switch (cProfile.getCsType().getDType()) {
      case LONG, OID, UINT32, UINT64, SERIAL, SMALLSERIAL, BIGSERIAL, BIGINT, INT64, INT128, INT256 ->
          dimensionDAO.getLongById(objIndex);

      default -> {
        try {
          yield dimensionDAO.getLongById(objIndex);
        } catch (Exception el) {
          log.info(el.getMessage());
          log.warn(cProfile);
          yield objIndex;
        }
      }
    };
  }

  public double convertIntToDouble(int objIndex,
                                   CProfile cProfile) {
    return switch (cProfile.getCsType().getDType()) {
      case FLOAT64, DECIMAL, FLOAT4, REAL, FLOAT8, FLOAT32, FLOAT, NUMERIC, MONEY, SMALLMONEY, DOUBLE ->
          dimensionDAO.getDoubleById(objIndex);

      default -> {
        try {
          yield dimensionDAO.getDoubleById(objIndex);
        } catch (Exception ed) {
          log.info(ed.getMessage());
          log.warn(cProfile);
          yield objIndex;
        }
      }
    };
  }

  public double convertIntFromDoubleLong(int objIndex,
                                         CProfile cProfile) {
    return switch (cProfile.getCsType().getDType()) {
      case UINT8, UINT16, INT16, INT2, INT4, INT8, INT32,
           NUMBER, INTEGER, SMALLINT, INT, TINYINT, BIT, TIME, TIMETZ -> objIndex;
      case FLOAT64, DECIMAL, FLOAT4, REAL, FLOAT8, FLOAT32, FLOAT, NUMERIC, MONEY, SMALLMONEY, DOUBLE ->
          dimensionDAO.getDoubleById(objIndex);
      case LONG, OID, UINT32, UINT64, SERIAL, SMALLSERIAL, BIGSERIAL, BIGINT, INT64, INT128, INT256 ->
          dimensionDAO.getLongById(objIndex);

      default -> {
        try {
          yield dimensionDAO.getDoubleById(objIndex);
        } catch (Exception ed) {
          log.info(ed.getMessage());
          log.warn(cProfile);
          try {
            yield dimensionDAO.getLongById(objIndex);
          } catch (Exception el) {
            log.info(el.getMessage());
            log.warn(cProfile);
            yield objIndex;
          }
        }
      }
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
          return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        } else if (obj instanceof LocalDate localDate) {
          return localDate.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();
        } else if (obj instanceof OffsetDateTime offsetDateTime) {
          return offsetDateTime.toInstant().toEpochMilli();
        } else if (obj instanceof ZonedDateTime zonedDateTime) {
          return zonedDateTime.toInstant().toEpochMilli();
        } else if (obj instanceof Date date) {
          return date.getTime();
        } else if (obj instanceof byte[] ba) {
          java.sql.Timestamp timestamp = new java.sql.Timestamp(
              java.nio.ByteBuffer.wrap(ba).getLong()
          );
          java.util.Date date = new java.util.Date(timestamp.getTime());
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

  private String getDateForLong(int key) {
    return Instant.ofEpochMilli(dimensionDAO.getLongById(key))
        .atZone(ZoneId.systemDefault())
        .format(DateHelper.FORMATTER);
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