package cn.scut.util.transfer;

import cn.scut.app.util.transfer.context.entity.TransferConfigs;
import cn.scut.app.util.transfer.navigator.Navigator;
import cn.scut.app.util.transfer.navigator.impl.UnknownNavigator;
import cn.scut.app.util.transfer.transformer.Transformer;
import cn.scut.app.util.transfer.utils.TransferDevUtil;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;
import javax.xml.transform.TransformerException;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

public class TransferConfigsUtilTest {

  @Test
  public void adapterConfigsTest() throws IOException {
    ClassPathResource adapterConfigFile = new ClassPathResource("TransferConfigs.yaml");
    JsonNode adapterConfig = TransferDevUtil.yamlMapper.readTree(
        adapterConfigFile.getFile());
    JsonNode          dagComponentNode = adapterConfig.get("transfer").get("dag_component");
    ArrayList<String> dagComponent     = new ArrayList<>();
    if (dagComponentNode.isArray()) {
      dagComponentNode.forEach(node -> dagComponent.add(node.asText()));
    } else {
      dagComponent.add(dagComponentNode.asText());
    }
    String dagOrder = adapterConfig.get("transfer").get("dag_order").asText();
    System.out.println(dagComponent);
    System.out.println(dagOrder);
    LinkedHashMap<String, String>    oneLevelMap      = new LinkedHashMap<>();
    LinkedHashMap<String, Navigator> adapterNavigator = new LinkedHashMap<>();
    LinkedHashMap<String, HashMap<String, Transformer>> transformStrategiesMap =
        new LinkedHashMap<>();
    dagComponent.forEach(dagComponentName -> {
      String actualName = adapterConfig.get(dagComponentName).get("name").asText();
      oneLevelMap.put(dagComponentName, actualName);
      String    navigatorClass = adapterConfig.get(dagComponentName).get("navigator").asText();
      Navigator navigator;
      try {
        navigator = (Navigator) Class.forName(navigatorClass).newInstance();
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
      HashMap<String, Transformer> transformStrategy = new HashMap<>();
      JsonNode strategiesNode = adapterConfig.get(dagComponentName)
          .get("strategies");
      StreamSupport.stream(
          Spliterators.spliteratorUnknownSize(
              strategiesNode.fieldNames(),
              Spliterator.ORDERED),
          false).forEach(strategyNodeName -> {
            Transformer transformer;
            try {
              transformer = (Transformer) Class.forName(
                  strategiesNode.get(strategyNodeName).asText()).newInstance();
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
              throw new RuntimeException(e);
            }
            transformStrategy.put(strategyNodeName, transformer);
          }
      );
      transformStrategiesMap.put(dagComponentName, transformStrategy);
      adapterNavigator.put(dagComponentName, navigator);
    });
    adapterNavigator.put("unknown", new UnknownNavigator());
    System.out.println(oneLevelMap);
    System.out.println(adapterNavigator);
    System.out.println(transformStrategiesMap);
  }

  @Test
  public void adapterConfigsUtilTest() throws TransformerException {
    TransferConfigs transferConfigs = new TransferConfigs();
    System.out.println(transferConfigs.getDagOrder());
    System.out.println(transferConfigs.getNavigatorStrategies("train"));
    System.out.println(transferConfigs.getLevelOneField("train"));
  }
}
