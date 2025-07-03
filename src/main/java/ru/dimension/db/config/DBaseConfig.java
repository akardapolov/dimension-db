package ru.dimension.db.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
@Data
@Builder(toBuilder = true)
public class DBaseConfig {

  private String configDirectory;
  private String configFileName = "metamodel.obj";
}
