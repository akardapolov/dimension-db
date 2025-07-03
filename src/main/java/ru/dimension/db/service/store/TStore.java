package ru.dimension.db.service.store;

import java.util.ArrayList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import ru.dimension.db.model.profile.CProfile;
import ru.dimension.db.service.CommonServiceApi;
import ru.dimension.db.util.CachedLastLinkedHashMap;

@Getter
@EqualsAndHashCode(callSuper = true)
public class TStore extends CommonServiceApi {

  private final int initialCapacity;

  private final List<List<Long>> rawData;
  private final CachedLastLinkedHashMap<Integer, Integer> mapping;

  public TStore(int initialCapacity,
                List<CProfile> cProfiles) {
    this.initialCapacity = initialCapacity;

    rawData = new ArrayList<>(this.initialCapacity);
    mapping = new CachedLastLinkedHashMap<>();

    fillArrayList(rawData, 1);
    fillTimestampMap(cProfiles, mapping);
  }

  public void add(int iC,
                  int iR,
                  long key) {
    this.rawData.get(mapping.get(iC)).add(iR, key);
  }

  public int size() {
    return this.rawData.get(0).size();
  }

  public long getBlockId() {
    return this.rawData.get(0).get(0);
  }

  public long getTail() {
    return this.rawData.get(0).get(this.rawData.get(0).size() - 1);
  }

  public int[] mappingToArray() {
    return this.mapping.keySet().stream().mapToInt(i -> i).toArray();
  }

  public long[][] dataToArray() {
    return getArrayLong(rawData);
  }
}
