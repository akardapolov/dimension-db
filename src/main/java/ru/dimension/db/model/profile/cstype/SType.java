package ru.dimension.db.model.profile.cstype;

import lombok.Getter;

@Getter
public enum SType {
  RAW((byte) 0x01),        // 1
  HISTOGRAM((byte) 0x02),  // 2
  ENUM((byte) 0x03);       // 3

  private final byte key;

  SType(byte key) {
    this.key = key;
  }
}
