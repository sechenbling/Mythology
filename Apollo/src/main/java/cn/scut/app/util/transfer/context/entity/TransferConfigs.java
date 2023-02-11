package cn.scut.app.util.transfer.context.entity;

import cn.scut.app.util.transfer.checker.Checker;
import cn.scut.app.util.transfer.navigator.Navigator;
import cn.scut.app.util.transfer.transformer.Transformer;
import cn.scut.app.util.transfer.utils.TransferConfigsUtil;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import javax.xml.transform.TransformerException;

public class TransferConfigs {

  private final ArrayList<String> dagComponents;
  private final LinkedHashMap<String, String> levelOneFieldsMap;

  private final LinkedHashMap<String, LinkedHashMap<String, String>> transferStagesTasksNames;
  private final LinkedHashMap<String, Checker> transferChecker;
  private final LinkedHashMap<String, Navigator> transferNavigator;
  private final LinkedHashMap<String, HashMap<String, Transformer>> transferNavigateTransformer;
  private final String dagOrder;

  public TransferConfigs() throws TransformerException {
    TransferConfigsUtil util = new TransferConfigsUtil();
    this.dagComponents = util.getDagComponents();
    this.levelOneFieldsMap = util.getLevelOneFieldsMap();
    this.transferStagesTasksNames = util.getTransferStagesTasksNames();
    this.transferChecker = util.getTransferChecker();
    this.transferNavigator = util.getTransferNavigator();
    this.transferNavigateTransformer = util.getTransferNavigateTransformer();
    this.dagOrder = util.getDagOrder();
  }

  public ArrayList<String> getDagComponents() {
    return dagComponents;
  }

  public String getLevelOneField(String signKey) {
    return levelOneFieldsMap.getOrDefault(signKey, "unknown");
  }

  public ArrayList<String> getDagOrder() {
    ArrayList<String> dagOrderList = new ArrayList<>();
    Arrays.stream(dagOrder.split("->")).forEach(dagComponentsName -> {
      if (dagComponents.contains(dagComponentsName)) {
        dagOrderList.add(dagComponentsName);
      }
    });
    return dagOrderList;
  }

  public Checker getTransferChecker(String signKey) {
    return this.transferChecker.containsKey(signKey) ? this.transferChecker.get(signKey) :
        this.transferChecker.get("nocheck");
  }

  public Navigator getTransferNavigator(String signKey) {
    return this.transferNavigator.containsKey(signKey) ? this.transferNavigator.get(signKey)
        : this.transferNavigator.get("unknown");
  }

  public HashMap<String, Transformer> getNavigatorStrategies(String signKey) {
    return this.transferNavigateTransformer.containsKey(signKey)
        ? this.transferNavigateTransformer.get(signKey) : new HashMap<>();
  }

  public LinkedHashMap<String, String> getTransferStagesTasksName(String signKey) {
    return this.transferStagesTasksNames.containsKey(signKey)
        ? this.transferStagesTasksNames.get(signKey)
        : new LinkedHashMap<>();
  }

  public LinkedHashMap<String, LinkedHashMap<String, String>> getTransferStagesTasks() {
    return this.transferStagesTasksNames;
  }
}
