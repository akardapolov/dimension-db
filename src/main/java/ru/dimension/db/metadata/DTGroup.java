package ru.dimension.db.metadata;

public enum DTGroup {
  INTEGER((byte) 1),
  FLOAT((byte) 2),
  STRING((byte) 3),
  DATETIME((byte) 4),
  BINARY((byte) 5),
  ARRAY((byte) 6),
  MAP((byte) 7),
  SET((byte) 8),
  BOOLEAN((byte) 9),
  NETWORK((byte) 10),
  JSON((byte) 11),
  SPATIAL((byte) 12),
  INTERVAL((byte) 13);

  private final byte key;

  DTGroup(byte key) {
    this.key = key;
  }

  public byte getKey() {
    return key;
  }
}