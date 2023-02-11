package cn.scut.app.util.transfer.utils;

import static cn.scut.app.util.transfer.constant.TransferConfigs.CHECKER;
import static cn.scut.app.util.transfer.constant.TransferConfigs.END;
import static cn.scut.app.util.transfer.constant.TransferConfigs.END_TASK_SUFFIX;
import static cn.scut.app.util.transfer.constant.TransferConfigs.NAME;
import static cn.scut.app.util.transfer.constant.TransferConfigs.NAVIGATOR;
import static cn.scut.app.util.transfer.constant.TransferConfigs.STAGES;
import static cn.scut.app.util.transfer.constant.TransferConfigs.START;
import static cn.scut.app.util.transfer.constant.TransferConfigs.START_TASK_SUFFIX;
import static cn.scut.app.util.transfer.constant.TransferConfigs.STRATEGIES;

import cn.scut.app.util.transfer.checker.Checker;
import cn.scut.app.util.transfer.checker.impl.UnknownChecker;
import cn.scut.app.util.transfer.navigator.Navigator;
import cn.scut.app.util.transfer.navigator.impl.UnknownNavigator;
import cn.scut.app.util.transfer.transformer.Transformer;
import com.fasterxml.jackson.databind.JsonNode;
import io.vavr.control.Either;
import io.vavr.control.Try;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.logging.Logger;
import javax.xml.transform.TransformerException;
import org.springframework.core.io.ClassPathResource;

public class TransferConfigsUtil {

  private final ArrayList<String>                                    dagComponents;
  private final LinkedHashMap<String, String>                        levelOneFieldsMap;
  private final LinkedHashMap<String, Checker>                       transferChecker;
  private final LinkedHashMap<String, LinkedHashMap<String, String>> transferStagesTasksNames;
  private final LinkedHashMap<String, Navigator>                     transferNavigator;
  private final LinkedHashMap<String, HashMap<String, Transformer>>  transferNavigateTransformer;
  private       String                                               dagOrder;

  public TransferConfigsUtil() throws TransformerException {
    Logger.getGlobal().info("== START CONFIGURE ADAPTER STRATEGIES ==");
    this.dagComponents              = new ArrayList<>();
    this.dagOrder                   = "";
    this.levelOneFieldsMap          = new LinkedHashMap<>();
    this.transferStagesTasksNames    = new LinkedHashMap<>();
    this.transferChecker             = new LinkedHashMap<>();
    this.transferNavigator           = new LinkedHashMap<>();
    this.transferNavigateTransformer = new LinkedHashMap<>();
    Either<Throwable, String> strategiesLoadEither = this.transferConfigsLoad();
    if (this.transferConfigsLoad().isLeft()) {
      throw new TransformerException(strategiesLoadEither.getLeft().getMessage());
    } else {
      Logger.getGlobal().info(strategiesLoadEither.get());
    }
    Logger.getGlobal().info("== END CONFIGURE ADAPTER STRATEGIES ==");
  }

  private Either<Throwable, String> transferConfigsLoad() {
    return Try.of(() -> {
      ClassPathResource transferConfigFile = new ClassPathResource("TransferConfigs.yaml");
      JsonNode          transferConfig     = TransferDevUtil.yamlMapper.readTree(
          transferConfigFile.getFile());
      this.configsInit(transferConfig);
      this.configsLoad(transferConfig);
      return "strategies are loaded successfully";
    }).toEither();
  }

  private void configsInit(JsonNode transferConfig) {
    JsonNode dagComponentNode = transferConfig.get("transfer").get("dag_component");
    if (dagComponentNode.isArray()) {
      dagComponentNode.forEach(node -> {
        if (transferConfig.has(node.asText())) {
          this.dagComponents.add(node.asText());
        }
      });
    } else {
      this.dagComponents.add(dagComponentNode.asText());
    }
    this.dagOrder = transferConfig.get("transfer").get("dag_order").asText();
  }

  private void configsLoad(JsonNode transferConfig)
      throws ClassNotFoundException {
    for (String dagComponentName : this.dagComponents) {
      if (transferConfig.has(dagComponentName)) {
        JsonNode dagComponentNode = transferConfig.get(dagComponentName);
        // stage: fields config
        String actualName = dagComponentNode.get(NAME).asText();
        levelOneFieldsMap.put(dagComponentName, actualName);
        // stage: stage task config
        LinkedHashMap<String, String> stagesTasksName = new LinkedHashMap<>();
        stagesTasksName.put(START, dagComponentName + START_TASK_SUFFIX);
        stagesTasksName.put(END, dagComponentName + END_TASK_SUFFIX);
        Optional.ofNullable(dagComponentNode.get(STAGES))
            .ifPresent(stagesNode -> {
              stagesTasksName.put(START, stagesNode.has(START)
                  ? stagesNode.get(START).asText() : dagComponentName + START_TASK_SUFFIX);
              stagesTasksName.put(END, stagesNode.has(END)
                  ? stagesNode.get(END).asText() : dagComponentName + END_TASK_SUFFIX);
            });
        transferStagesTasksNames.put(dagComponentName, stagesTasksName);
        // stage: stage checker config
        Checker checker = Try.of(() -> {
          if (dagComponentNode.has(CHECKER)) {
            String dagComponentCheckerClass = dagComponentNode.get(CHECKER).asText();
            return (Checker) Class.forName(dagComponentCheckerClass).newInstance();
          } else {
            return new UnknownChecker();
          }
        }).get();
        transferChecker.put(dagComponentName, checker);
        // stage: stage strategies config
        String                       navigatorClass    = dagComponentNode.get(NAVIGATOR).asText();
        HashMap<String, Transformer> transformStrategy = new HashMap<>();
        JsonNode                     strategiesNode    = dagComponentNode.get(STRATEGIES);
        Iterator<String>             fieldNamesIter    = strategiesNode.fieldNames();
        while (fieldNamesIter.hasNext()) {
          String strategyNodeName = fieldNamesIter.next();
          String strategiesClass  = strategiesNode.get(strategyNodeName).asText();
          Transformer transformer = Try.of(() -> (Transformer) Class.forName(
                  strategiesClass).newInstance())
              .getOrElseThrow(() -> new ClassNotFoundException(
                  "Transfer strategy class not found: " + strategiesClass));
          transformStrategy.put(strategyNodeName, transformer);
        }
        transferNavigateTransformer.put(dagComponentName, transformStrategy);
        // stage: stage navigator config
        Navigator navigator = Try.of(() -> (Navigator) Class.forName(navigatorClass).newInstance())
            .getOrElseThrow(() -> new ClassNotFoundException(
                "Transfer navigator class not found: " + navigatorClass));
        navigator.setNavigatorStrategies(
            transferNavigateTransformer.getOrDefault(dagComponentName, new HashMap<>()));
        transferNavigator.put(dagComponentName, navigator);
      }
    }
    this.transferChecker.put("unknown", new UnknownChecker());
    this.transferNavigator.put("unknown", new UnknownNavigator());
  }

  public ArrayList<String> getDagComponents() {
    return dagComponents;
  }

  public LinkedHashMap<String, String> getLevelOneFieldsMap() {
    return levelOneFieldsMap;
  }

  public LinkedHashMap<String, LinkedHashMap<String, String>> getTransferStagesTasksNames() {
    return transferStagesTasksNames;
  }

  public LinkedHashMap<String, Checker> getTransferChecker() {
    return transferChecker;
  }

  public LinkedHashMap<String, Navigator> getTransferNavigator() {
    return transferNavigator;
  }

  public LinkedHashMap<String, HashMap<String, Transformer>> getTransferNavigateTransformer() {
    return transferNavigateTransformer;
  }

  public String getDagOrder() {
    return dagOrder;
  }
}
