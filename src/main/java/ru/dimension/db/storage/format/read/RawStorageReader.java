package ru.dimension.db.storage.format.read;

import ru.dimension.db.metadata.DataType;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.cstype.CType;
import ru.dimension.db.model.profile.cstype.SType;
import ru.dimension.db.service.mapping.Mapper;
import ru.dimension.db.storage.RawDAO;
import ru.dimension.db.storage.format.DualColumnProcessor;
import ru.dimension.db.storage.format.StorageContext;
import ru.dimension.db.storage.format.StorageReader;
import ru.dimension.db.util.DateHelper;

public class RawStorageReader implements StorageReader {

  @Override
  public String getName() {
    return "RAW";
  }

  @Override
  public boolean supports(SType sType) {
    return SType.RAW.equals(sType);
  }

  @Override
  public String[] getStringValues(StorageContext context, CProfile cProfile) {
    return getStringArrayValuesRaw(context.getRawDAO(), context.getTableId(),
                                   context.getBlockId(), cProfile);
  }

  @Override
  public double[] getDoubleValues(StorageContext context, CProfile cProfile) {
    return getDoubleArrayValuesRaw(context.getRawDAO(), context.getTableId(),
                                   context.getBlockId(), cProfile);
  }

  @Override
  public int[] getIntValues(StorageContext context, CProfile cProfile) {
    throw new UnsupportedOperationException("RAW format does not provide int values");
  }

  @Override
  public void processDualColumns(StorageContext context,
                                 CProfile firstProfile,
                                 CProfile secondProfile,
                                 DualColumnProcessor processor) {
    String[] firstValues = getStringValues(context, firstProfile);
    String[] secondValues = getStringValues(context, secondProfile);

    for (int i = 0; i < context.getTimestamps().length; i++) {
      processor.process(firstValues[i], secondValues[i]);
    }
  }

  public String[] getStringArrayValuesRaw(RawDAO rawDAO,
                                          byte tableId,
                                          long blockId,
                                          CProfile cProfile) {
    int colId = cProfile.getColId();
    CType cType = Mapper.isCType(cProfile);
    switch (cType) {
      case INT -> {
        int[] intVals = rawDAO.getRawInt(tableId, blockId, colId);
        String[] resI = new String[intVals.length];
        for (int i = 0; i < intVals.length; i++) {
          resI[i] = (intVals[i] == Mapper.INT_NULL) ? "" : String.valueOf(intVals[i]);
        }
        return resI;
      }
      case LONG -> {
        long[] longVals = rawDAO.getRawLong(tableId, blockId, colId);
        String[] resL = new String[longVals.length];
        for (int i = 0; i < longVals.length; i++) {
          if (longVals[i] == Mapper.LONG_NULL) {
            resL[i] = "";
          } else if (isDateTimeType(cProfile.getCsType().getDType())) {
            resL[i] = getDateForLongShorted(Math.toIntExact(longVals[i] / 1000));
          } else {
            resL[i] = String.valueOf(longVals[i]);
          }
        }
        return resL;
      }
      case FLOAT -> {
        float[] floatVals = rawDAO.getRawFloat(tableId, blockId, colId);
        String[] resF = new String[floatVals.length];
        for (int i = 0; i < floatVals.length; i++) {
          resF[i] = (floatVals[i] == Mapper.FLOAT_NULL) ? "" : String.valueOf(floatVals[i]);
        }
        return resF;
      }
      case DOUBLE -> {
        double[] doubleVals = rawDAO.getRawDouble(tableId, blockId, colId);
        String[] resD = new String[doubleVals.length];
        for (int i = 0; i < doubleVals.length; i++) {
          resD[i] = (doubleVals[i] == Mapper.DOUBLE_NULL) ? "" : String.valueOf(doubleVals[i]);
        }
        return resD;
      }
      case STRING -> {
        String[] strVals = rawDAO.getRawString(tableId, blockId, colId);
        for (int i = 0; i < strVals.length; i++) {
          if (strVals[i] == null)
            strVals[i] = "";
        }
        return strVals;
      }
      default -> {
        return new String[0];
      }
    }
  }

  protected double[] getDoubleArrayValuesRaw(RawDAO rawDAO,
                                             byte tableId,
                                             long blockId,
                                             CProfile cProfile) {
    int colId = cProfile.getColId();
    CType cType = Mapper.isCType(cProfile);
    switch (cType) {
      case INT -> {
        int[] vals = rawDAO.getRawInt(tableId, blockId, colId);
        double[] res = new double[vals.length];
        for (int i = 0; i < vals.length; i++)
          res[i] = (vals[i] == Mapper.INT_NULL) ? 0 : vals[i];
        return res;
      }
      case LONG -> {
        long[] vals = rawDAO.getRawLong(tableId, blockId, colId);
        double[] res = new double[vals.length];
        for (int i = 0; i < vals.length; i++)
          res[i] = (vals[i] == Mapper.LONG_NULL) ? 0 : vals[i];
        return res;
      }
      case FLOAT -> {
        float[] vals = rawDAO.getRawFloat(tableId, blockId, colId);
        double[] res = new double[vals.length];
        for (int i = 0; i < vals.length; i++)
          res[i] = (vals[i] == Mapper.FLOAT_NULL) ? 0 : vals[i];
        return res;
      }
      case DOUBLE -> {
        double[] vals = rawDAO.getRawDouble(tableId, blockId, colId);
        double[] res = new double[vals.length];
        for (int i = 0; i < vals.length; i++)
          res[i] = (vals[i] == Mapper.DOUBLE_NULL) ? 0 : vals[i];
        return res;
      }
      default -> {
        return new double[0];
      }
    }
  }

  private boolean isDateTimeType(DataType type) {
    return type == DataType.TIMESTAMP ||
           type == DataType.TIMESTAMPTZ ||
           type == DataType.DATETIME ||
           type == DataType.DATETIME2 ||
           type == DataType.SMALLDATETIME;
  }

  private String getDateForLongShorted(int longDate) {
    return DateHelper.format(longDate);
  }
}