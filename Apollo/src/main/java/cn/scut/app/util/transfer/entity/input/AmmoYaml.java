package cn.scut.app.util.transfer.entity.input;

import cn.scut.app.util.transfer.utils.TransferDevUtil;
import com.fasterxml.jackson.databind.JsonNode;
import io.vavr.control.Try;
import java.util.LinkedHashMap;
import java.util.Map;

public class AmmoYaml {

  AmmoMetadata metadata;
  String       config;
  String       domain;
  String       owner;

  public AmmoYaml(AmmoMetadata metadata, String config, String owner) {
    this.metadata = metadata;
    this.config   = config;
    this.domain   = "ammo";
    this.owner    = owner;
  }

  public Map<String, Object> getAmmoYamlMap() {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("metadata", metadata.getAmmonMetaMap());
    map.put("domain", domain);
    map.put("owner", owner);
    map.putAll(TransferDevUtil.yaml.load(this.config));
    return map;
  }

  public JsonNode getAmmoYamlJsonNode() {
    return Try.of(() -> TransferDevUtil.yamlMapper.readTree(
            TransferDevUtil.yamlMapper.writeValueAsString(this.getAmmoYamlMap())))
        .get();
  }

  public String getAmmoYamlString() {
    return Try.of(() -> TransferDevUtil.yamlMapper.writeValueAsString(this.getAmmoYamlMap())).get();
  }
}
