package cn.scut.app.util.transfer.entity.input;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AmmoMetadata {

  String                                         project;
  List<Map<String, ArrayList<String>>> models;
  Map<String, Map<String, Object>>               offlineInfo;
  Map<String, Map<String, Object>>               onlineInfo;
  Set<String>                                    owners;

  public AmmoMetadata(String project) {
    this.project = project;
    models       = new ArrayList<>();
    offlineInfo  = new HashMap<>();
    onlineInfo   = new HashMap<>();
    owners       = new HashSet<>();
  }

  public void setModels(List<Map<String, ArrayList<String>>> models) {
    this.models = models;
  }

  public void setOfflineInfo(Map<String, Map<String, Object>> offlineInfo) {
    this.offlineInfo = offlineInfo;
  }

  public void setOnlineInfo(Map<String, Map<String, Object>> onlineInfo) {
    this.onlineInfo = onlineInfo;
  }

  public void setOwners(Set<String> owners) {
    this.owners = owners;
  }

  public void setMetadata(List<Map<String, ArrayList<String>>> models,
      Map<String, Map<String, Object>> offlineInfo, Map<String, Map<String, Object>> onlineInfo,
      Set<String> owners) {
    this.models      = models;
    this.offlineInfo = offlineInfo;
    this.onlineInfo  = onlineInfo;
    this.owners      = owners;
  }

  public Map<String, Object> getAmmonMetaMap() {
    Map<String, Object> ammoMetaMap = new LinkedHashMap<>();
    ammoMetaMap.put("project", this.project);
    ammoMetaMap.put("models", this.models);
    ammoMetaMap.put("offline_info", this.offlineInfo);
    ammoMetaMap.put("online_info", this.onlineInfo);
    ammoMetaMap.put("owners", this.owners);
    return ammoMetaMap;
  }
}
