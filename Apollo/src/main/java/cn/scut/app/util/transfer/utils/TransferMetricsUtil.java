package cn.scut.app.util.transfer.utils;

import static cn.scut.app.util.transfer.constant.EvalStageFields.DEFAULT;
import static cn.scut.app.util.transfer.constant.EvalStageFields.EXPRESSION;
import static cn.scut.app.util.transfer.constant.EvalStageFields.METRICS_NAME;
import static cn.scut.app.util.transfer.constant.EvalStageFields.NAME;
import static cn.scut.app.util.transfer.constant.EvalStageFields.PLACEHOLDER;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.List;

public class TransferMetricsUtil {

  public static List<JsonNode> analysisMetricsNode(JsonNode metric, String... metricsName)
      throws JsonProcessingException {
    List<JsonNode> metricsInstanceList = new ArrayList<>();
    JsonNode names = metric.get(
        metricsName.length == 0 ? NAME : metricsName[0]);
    if (!names.isArray()) {
      String name = names.asText();
      metricsInstanceList.add(analysisMetricsNodeGeneration(name, metric));
    } else {
      for (JsonNode name : names) {
        metricsInstanceList.add(analysisMetricsNodeGeneration(name.asText(), metric));
      }
    }
    return metricsInstanceList;
  }

  private static JsonNode analysisMetricsNodeGeneration(String name, JsonNode metric)
      throws JsonProcessingException {
    ObjectNode metricsInstance = JsonNodeFactory.instance.objectNode();
    String     expression      = metric.get(EXPRESSION).asText();
    metricsInstance.replace(METRICS_NAME, JsonNodeFactory.instance.textNode(name));
    metricsInstance.replace(EXPRESSION, JsonNodeFactory.instance.textNode(expression));
    if (metric.has(DEFAULT)) {
      JsonNode placeholder = TransferDevUtil.yamlMapper.readTree(
          TransferDevUtil.yamlMapper.writeValueAsString(metric.get(DEFAULT)));
      metricsInstance.replace(PLACEHOLDER, placeholder);
    }
    return metricsInstance;
  }

  public static JsonNode analysisMetricsNames(JsonNode metrics) {
    ArrayNode metricsNames = JsonNodeFactory.instance.arrayNode();
    if (!metrics.isArray()) {
      analysisMetricsNameGeneration(metrics).forEach(metricsNames::add);
    } else {
      for (JsonNode metric : metrics) {
        analysisMetricsNameGeneration(metric).forEach(metricsNames::add);
      }
    }
    return metricsNames;
  }

  private static List<String> analysisMetricsNameGeneration(JsonNode metric) {
    List<String> metricsNameList = new ArrayList<>();
    JsonNode     names           = metric.get(NAME);
    if (!names.isArray()) {
      String name = names.asText();
      metricsNameList.add(name);
    } else {
      for (JsonNode name : names) {
        String nameStr = name.asText();
        metricsNameList.add(nameStr);
      }
    }
    return metricsNameList;
  }

  public static List<JsonNode> analysisMetricsList(JsonNode conditions)
      throws JsonProcessingException {
    List<JsonNode> conditionList = new ArrayList<>();
    for (JsonNode condition : conditions) {
      conditionList.addAll(analysisMetricsNode(condition, METRICS_NAME));
    }
    return conditionList;
  }
}
