package ru.dimension.db.service.mapping;

import static java.lang.String.valueOf;
import static ru.dimension.db.util.MapArrayUtil.arrayToString;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;
import lombok.extern.log4j.Log4j2;
import ru.dimension.db.metadata.DataType;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.cstype.CType;
import ru.dimension.db.model.profile.cstype.SType;
import ru.dimension.db.storage.helper.ClickHouseHelper;

@Log4j2
public class Mapper {

  public static int INT_NULL = Integer.MIN_VALUE;
  public static long LONG_NULL = Long.MIN_VALUE;
  public static float FLOAT_NULL = Float.MIN_VALUE;
  public static double DOUBLE_NULL = Double.MIN_VALUE;
  public static String STRING_NULL = "";

  public Mapper() {
  }

  public static CType isCType(CProfile cProfile) {
    String typeName = cProfile.getColDbTypeName().toUpperCase();
    
    if (typeName.contains("FIXEDSTRING")) return CType.STRING;
    if (typeName.contains("ENUM")) return CType.STRING;
    if (typeName.contains("SET")) return CType.STRING;

    if (typeName.contains("DECIMAL")) return CType.DOUBLE;
    if (typeName.contains("DATETIME")) return CType.LONG;
    if (typeName.contains("NULLABLE")) return CType.STRING;
    if (typeName.contains("ARRAY")) return CType.STRING;
    if (typeName.contains("MAP")) return CType.STRING;

    DataType dataType = isDBType(cProfile);
    return switch (dataType) {
      case UINT8, UINT16, INT16, INT2, INT4, INT8, INT32,
          NUMBER, INTEGER, SMALLINT, INT, BIGINT, BIT, TIME, TIMETZ, TINYINT ->
          CType.INT;
      case OID, DATE, TIMESTAMP, TIMESTAMPTZ, DATETIME, DATETIME2, SMALLDATETIME,
          UINT32, UINT64, INT64, INT128, INT256,
          SERIAL, SMALLSERIAL, BIGSERIAL, LONG ->
          CType.LONG;
      case FLOAT4, FLOAT32, REAL -> CType.FLOAT;
      case FLOAT64, NUMERIC, FLOAT, FLOAT8, MONEY, SMALLMONEY, DECIMAL, DOUBLE -> CType.DOUBLE;
      default -> CType.STRING;
    };
  }

  public static DataType isDBType(CProfile cProfile) {
    String typeName = cProfile.getColDbTypeName().toUpperCase();

    if (typeName.contains("UNSIGNED")) {
      if (typeName.contains("TINYINT")) return DataType.UINT8;
      if (typeName.contains("SMALLINT")) return DataType.UINT16;
      if (typeName.contains("INT") || typeName.contains("INTEGER")) return DataType.UINT32;
      if (typeName.contains("BIGINT")) return DataType.UINT64;
    }

    if (typeName.contains("DECIMAL")) return DataType.DECIMAL;
    if (typeName.contains("FIXEDSTRING")) return DataType.FIXEDSTRING;
    if (typeName.contains("ENUM8")) return DataType.ENUM8;
    if (typeName.contains("ENUM16")) return DataType.ENUM16;
    if (typeName.contains("ENUM")) return DataType.ENUM;

    if (typeName.equals("DATETIME")) return DataType.DATETIME;
    if (typeName.equals("DATETIME2")) return DataType.DATETIME2;
    if (typeName.contains("DATETIME")) return DataType.DATETIME;

    if (typeName.contains("NULLABLE")) return DataType.NULLABLE;
    if (typeName.contains("ARRAY")) return DataType.ARRAY;
    if (typeName.contains("MAP")) return DataType.MAP;

    if (typeName.contains("SET")) return DataType.SET;

    try {
      return DataType.valueOf(typeName);
    } catch (IllegalArgumentException e) {
      log.warn("Unknown data type: {}, fallback to STRING", typeName);
      return DataType.STRING;
    }
  }

  public static int convertRawToInt(Object obj,
                                    CProfile cProfile) {
    if (obj == null) {
      return INT_NULL;
    }
    switch (cProfile.getCsType().getDType()) {
      case UINT8:
      case UINT16:
      case INT16:
      case INT32:
      case INT2:
      case INT4:
      case INT8:
      case NUMBER:
      case INTEGER:
      case SMALLINT:
      case INT:
      case BIGINT:
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
      default:
        return INT_NULL;
    }
  }

  public static long convertRawToLong(Object obj,
                                      CProfile cProfile) {
    if (obj == null) {
      return LONG_NULL;
    }

    switch (cProfile.getCsType().getDType()) {
      case DATE:
      case TIMESTAMP:
      case TIMESTAMPTZ:
      case DATETIME:
      case DATETIME2:
      case SMALLDATETIME:
        if (obj instanceof Timestamp ts) {
          return ts.getTime();
        } else if (obj instanceof Instant instant) {
          return instant.toEpochMilli();
        } else if (obj instanceof LocalDateTime localDateTime) {
          return localDateTime.atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
        } else if (obj instanceof LocalDate localDate) {
          return localDate.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        } else if (obj instanceof Date date) {
          return date.getTime();
        } else if (obj instanceof byte[] ba) {
          java.sql.Timestamp timestamp = new java.sql.Timestamp(
              java.nio.ByteBuffer.wrap(ba).getLong()
          );
          java.util.Date date = new java.util.Date(timestamp.getTime());
          return date.getTime();
        } else if (obj instanceof OffsetDateTime offsetDateTime) {
          return offsetDateTime.toInstant().toEpochMilli();
        }
      case OID:
      case BIGSERIAL:
      case UINT32:
      case INT64:
      case UINT64:
      case LONG:
        if (obj instanceof Long) {
          return (Long) obj;
        } else if (ClickHouseHelper.checkUnsigned(obj.getClass().getName())) {
          return ClickHouseHelper.invokeMethod(obj, "longValue", Long.class);
        }
        return ((Number) obj).longValue();
      case INT128:
      case INT256:
        if (obj instanceof BigInteger bigInteger) {
          return bigInteger.longValue();
        }
      case SERIAL:
      case SMALLSERIAL:
        if (obj instanceof Integer i) {
          return i;
        }
      default:
        return LONG_NULL;
    }
  }

  public static float convertRawToFloat(Object obj,
                                        CProfile cProfile) {
    if (obj == null) {
      return FLOAT_NULL;
    }
    if (cProfile.getCsType().getDType() == DataType.FLOAT32) {
      return (Float) obj;
    } else if (cProfile.getCsType().getDType() == DataType.FLOAT4) {
      return (Float) obj;
    } else if (cProfile.getCsType().getDType() == DataType.REAL) {
      return (Float) obj;
    }
    return FLOAT_NULL;
  }

  public static double convertRawToDouble(Object obj,
                                          CProfile cProfile) {
    if (obj == null) {
      return DOUBLE_NULL;
    }

    switch (cProfile.getCsType().getDType()) {
      case FLOAT64:
      case FLOAT8:
      case DOUBLE:
        return (Double) obj;
      case MONEY:
      case FLOAT:
      case DECIMAL:
      case NUMERIC:
      case SMALLMONEY:
        if (obj instanceof BigDecimal bd) {
          return bd.doubleValue();
        } else if (obj instanceof Float fl) {
          return fl.doubleValue();
        } else {
          return (Double) obj;
        }
      default:
        return DOUBLE_NULL;
    }
  }

  public static String convertRawToString(Object obj,
                                          CProfile cProfile) {
    if (obj == null) {
      return STRING_NULL;
    }

    switch (cProfile.getCsType().getDType()) {
      case ENUM8:
      case ENUM16:
      case FIXEDSTRING:
      case BPCHAR:
      case NAME:
      case TEXT:
      case NTEXT:
      case VARCHAR:
      case NVARCHAR2:
      case VARCHAR2:
      case NVARCHAR:
      case NULLABLE:
      case SET:
        return (String) obj;
      case ARRAY:
        if (obj.getClass().isArray()) {
          return arrayToString(obj);
        }
      case MAP:
        if (obj instanceof LinkedHashMap map) {
          return String.valueOf(map);
        } else {
          Map<?, ?> mapValue = (Map<?, ?>) obj;
          return String.valueOf(mapValue);
        }
      case IPV4:
        Inet4Address inet4Address = (Inet4Address) obj;
        return inet4Address.getHostAddress();
      case IPV6:
        Inet6Address inet6Address = (Inet6Address) obj;
        return inet6Address.getHostAddress();
      case NCHAR:
      case CHAR:
      case SYSNAME:
        String v = (String) obj;
        return v.trim();
      case OID:
        return valueOf(obj);
      case INT2:
      case INT4:
      case INT8:
      case TINYINT:
        Integer intVal = (Integer) obj;
        return valueOf(intVal);
      case FLOAT4:
      case REAL:
      case FLOAT8:
      case FLOAT32:
        Float f = (Float) obj;
        return valueOf(f);
      case FLOAT64:
      case DOUBLE:
        Double d = (Double) obj;
        return valueOf(d);
      case NUMBER:
        BigDecimal bgDec = (BigDecimal) obj;
        return valueOf(bgDec);
      case NCLOB:
      case CLOB:
        Clob clobVal = (Clob) obj;
        try {
          return clobVal.getSubString(1, (int) clobVal.length());
        } catch (SQLException e) {
          log.info("No data found while clob processing");
          return "No clob data";
        }
      case BOOL:
        if (obj instanceof Boolean b) {
          return b.toString();
        }
      case STRING:
      case UNIQUEIDENTIFIER:
        return (String) obj;
      case UUID:
        if (obj instanceof UUID u) {
          return u.toString();
        }
      case RAW:
      case BYTEA:
      case BINARY:
      case VARBINARY:
        return new String((byte[]) obj, StandardCharsets.UTF_8);
      case JSONB, POINT, INTERVAL:
        return obj.toString();
      default:
        return STRING_NULL;
    }
  }

  public static int getColumnCount(List<CProfile> cProfiles,
                                   Map<Integer, SType> colIdSTypeMap,
                                   Predicate<CProfile> isNotTimestamp,
                                   Predicate<CProfile> isCustom) {

    return (int) cProfiles.stream()
        .filter(isNotTimestamp)
        .filter(f -> SType.RAW.equals(colIdSTypeMap.get(f.getColId())))
        .filter(isCustom)
        .count();
  }

  public static int getColumnCount(List<CProfile> cProfiles,
                                   Predicate<CProfile> isRaw,
                                   Predicate<CProfile> isCustom) {

    return (int) cProfiles.stream()
        .filter(isRaw)
        .filter(isCustom)
        .count();
  }
}
