package ru.dimension.db.storage.format.read;

import java.util.HashMap;
import java.util.Map;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.model.profile.cstype.SType;
import ru.dimension.db.storage.format.StorageContext;
import ru.dimension.db.storage.format.StorageReader;
import ru.dimension.db.storage.format.DualColumnProcessor;

public class HistogramStorageReader implements StorageReader {

  @Override
  public String getName() {
    return "HISTOGRAM";
  }

  @Override
  public boolean supports(SType sType) {
    return SType.HISTOGRAM.equals(sType);
  }

  @Override
  public String[] getStringValues(StorageContext context, CProfile cProfile) {
    int[][] histograms = context.getHistogramDAO().get(
        context.getTableId(), context.getBlockId(), cProfile.getColId());
    int[] unpacked = getHistogramUnPack(context.getTimestamps(), histograms);

    String[] result = new String[context.getTimestamps().length];

    Map<Integer, String> cache = new HashMap<>();
    for (int i = 0; i < context.getTimestamps().length; i++) {
      int code = unpacked[i];
      String s = cache.computeIfAbsent(code, k -> context.getConverter().convertIntToRaw(k, cProfile));
      result[i] = s;
    }
    return result;
  }

  @Override
  public double[] getDoubleValues(StorageContext context, CProfile cProfile) {
    int[][] histograms = context.getHistogramDAO().get(
        context.getTableId(), context.getBlockId(), cProfile.getColId());
    int[] unpacked = getHistogramUnPack(context.getTimestamps(), histograms);

    double[] result = new double[context.getTimestamps().length];
    for (int i = 0; i < context.getTimestamps().length; i++) {
      result[i] = context.getConverter().convertIntFromDoubleLong(unpacked[i], cProfile);
    }
    return result;
  }

  @Override
  public int[] getIntValues(StorageContext context, CProfile cProfile) {
    int[][] histograms = context.getHistogramDAO().get(
        context.getTableId(), context.getBlockId(), cProfile.getColId());
    return getHistogramUnPack(context.getTimestamps(), histograms);
  }

  @Override
  public void processDualColumns(StorageContext context,
                                 CProfile firstProfile,
                                 CProfile secondProfile,
                                 DualColumnProcessor processor) {
    int[][] firstHist = context.getHistogramDAO().get(
        context.getTableId(), context.getBlockId(), firstProfile.getColId());
    int[][] secondHist = context.getHistogramDAO().get(
        context.getTableId(), context.getBlockId(), secondProfile.getColId());

    int[] firstUnpacked = getHistogramUnPack(context.getTimestamps(), firstHist);
    int[] secondUnpacked = getHistogramUnPack(context.getTimestamps(), secondHist);

    for (int i = 0; i < context.getTimestamps().length; i++) {
      processor.process(firstUnpacked[i], secondUnpacked[i]);
    }
  }

  protected int[] getHistogramUnPack(long[] timestamps,
                                     int[][] histograms) {
    int n = timestamps.length;
    if (n == 0) {
      return new int[0];
    }

    int[] indices = histograms[0];
    int[] values = histograms[1];
    int[] unpacked = new int[n];

    if (indices.length == 0) {
      return unpacked;
    }

    int currentIndex = 0;
    int currentValue = values[0];

    for (int i = 0; i < n; i++) {
      if (currentIndex + 1 < indices.length && i >= indices[currentIndex + 1]) {
        currentIndex++;
        currentValue = values[currentIndex];
      }
      unpacked[i] = currentValue;
    }

    return unpacked;
  }
}