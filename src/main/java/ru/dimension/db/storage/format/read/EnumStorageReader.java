package ru.dimension.db.storage.format.read;

import java.util.HashMap;
import java.util.Map;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.cstype.SType;
import ru.dimension.db.storage.format.StorageContext;
import ru.dimension.db.storage.format.StorageReader;
import ru.dimension.db.storage.format.DualColumnProcessor;
import ru.dimension.db.storage.bdb.entity.column.EColumn;
import ru.dimension.db.storage.helper.EnumHelper;

public class EnumStorageReader implements StorageReader {

  @Override
  public String getName() {
    return "ENUM";
  }

  @Override
  public boolean supports(SType sType) {
    return SType.ENUM.equals(sType);
  }

  @Override
  public String[] getStringValues(StorageContext context, CProfile cProfile) {
    EColumn eColumn = context.getEnumDAO().getEColumnValues(
        context.getTableId(), context.getBlockId(), cProfile.getColId());

    int[] dict = eColumn.getValues();
    byte[] data = eColumn.getDataByte();

    String[] result = new String[context.getTimestamps().length];

    Map<Integer, String> cache = new HashMap<>();
    for (int i = 0; i < context.getTimestamps().length; i++) {
      int intValue = EnumHelper.getIndexValue(dict, data[i]);
      String s = cache.computeIfAbsent(intValue, k -> context.getConverter().convertIntToRaw(k, cProfile));
      result[i] = s;
    }
    return result;
  }

  @Override
  public double[] getDoubleValues(StorageContext context, CProfile cProfile) {
    EColumn eColumn = context.getEnumDAO().getEColumnValues(
        context.getTableId(), context.getBlockId(), cProfile.getColId());

    int[] dict = eColumn.getValues();
    byte[] data = eColumn.getDataByte();

    double[] result = new double[context.getTimestamps().length];
    for (int i = 0; i < context.getTimestamps().length; i++) {
      int intValue = EnumHelper.getIndexValue(dict, data[i]);
      result[i] = context.getConverter().convertIntFromDoubleLong(intValue, cProfile);
    }
    return result;
  }

  @Override
  public int[] getIntValues(StorageContext context, CProfile cProfile) {
    EColumn eColumn = context.getEnumDAO().getEColumnValues(
        context.getTableId(), context.getBlockId(), cProfile.getColId());

    int n = context.getTimestamps().length;
    int[] dict = eColumn.getValues();
    byte[] bytes = eColumn.getDataByte();

    int[] ints = new int[n];
    for (int i = 0; i < n; i++) {
      ints[i] = EnumHelper.getIndexValue(dict, bytes[i]);
    }
    return ints;
  }

  @Override
  public void processDualColumns(StorageContext context,
                                 CProfile firstProfile,
                                 CProfile secondProfile,
                                 DualColumnProcessor processor) {
    EColumn firstColumn = context.getEnumDAO().getEColumnValues(
        context.getTableId(), context.getBlockId(), firstProfile.getColId());
    EColumn secondColumn = context.getEnumDAO().getEColumnValues(
        context.getTableId(), context.getBlockId(), secondProfile.getColId());

    for (int i = 0; i < context.getTimestamps().length; i++) {
      int firstValue = EnumHelper.getIndexValue(firstColumn.getValues(), firstColumn.getDataByte()[i]);
      int secondValue = EnumHelper.getIndexValue(secondColumn.getValues(), secondColumn.getDataByte()[i]);
      processor.process(firstValue, secondValue);
    }
  }
}