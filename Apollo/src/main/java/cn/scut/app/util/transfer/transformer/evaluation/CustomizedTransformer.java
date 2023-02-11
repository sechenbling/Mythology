package cn.scut.app.util.transfer.transformer.evaluation;

import static cn.scut.app.util.transfer.constant.EvalStageFields.DELIMITER;
import static cn.scut.app.util.transfer.constant.EvalStageFields.GROUP_DAG;
import static cn.scut.app.util.transfer.constant.EvalStageFields.GROUP_MODEL;
import static cn.scut.app.util.transfer.constant.EvalStageFields.LEVEL;
import static cn.scut.app.util.transfer.constant.EvalStageFields.RENDER;
import static cn.scut.app.util.transfer.constant.EvalStageFields.SOURCE_DAG;
import static cn.scut.app.util.transfer.constant.EvalStageFields.TASKS;
import static cn.scut.app.util.transfer.constant.EvalStageFields.TEMPLATE;
import static cn.scut.app.util.transfer.constant.TransferTaskFields.DEPENDS_ON;
import static cn.scut.app.util.transfer.constant.TransferTaskFields.DISABLE;

import cn.scut.app.util.transfer.context.TransferContext;
import cn.scut.app.util.transfer.entity.output.layer.impl.TransferGroup;
import cn.scut.app.util.transfer.entity.output.layer.impl.TransferTask;
import cn.scut.app.util.transfer.transformer.Transformer;
import cn.scut.app.util.transfer.utils.TransferDevUtil;
import cn.scut.app.util.transfer.utils.TransferRenderUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.vavr.control.Either;
import io.vavr.control.Try;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;
import javax.xml.transform.TransformerException;

public class CustomizedTransformer implements Transformer {

  private String level;

  public TransferGroup transform(HashMap<String, Object> root,
      TransferContext context) {
    return Try.of(() -> {
      JsonNode custom = TransferDevUtil.yamlMapper.readTree(
          TransferDevUtil.yamlMapper.writeValueAsString(root));
      Either<Throwable, Boolean> transCheckResult = check(root);
      transCheckResult.getOrElseThrow(
          () -> new TransformerException(transCheckResult.getLeft().getMessage()));
      List<TransferTask> adapterTaskList = new ArrayList<>();
      this.customizedTransform(custom)
          .forEach((key, value) -> adapterTaskList.add(new TransferTask(key, value)));
      String        groupId      = level.equals(SOURCE_DAG) ? GROUP_DAG : GROUP_MODEL;
      TransferGroup adapterGroup = new TransferGroup(groupId, adapterTaskList);
      adapterGroup.createRangeTask(groupId);
      return adapterGroup;
    }).get();
  }

  public Either<Throwable, Boolean> check(HashMap<String, Object> root) {
    return Try.of(() -> {
      if (!root.containsKey(TASKS)) {
        throw new TransformerException("post tasks loss key 'tasks'");
      }
      return true;
    }).toEither();
  }

  private LinkedHashMap<String, Object> customizedTransform(JsonNode custom) throws Exception {
    JsonNode tasks = custom.get(TASKS);
    this.level = custom.has(LEVEL) ? custom.get(LEVEL).asText() : SOURCE_DAG;
    if (!Optional.ofNullable(tasks).isPresent() || tasks == JsonNodeFactory.instance.nullNode()) {
      throw new TransformerException("Evaluation customized task loss key 'tasks'");
    }
    LinkedHashMap<String, Object> taskList = new LinkedHashMap<>();
    this.customizedTasksFormat(tasks);
    if (!Optional.ofNullable(custom.get(RENDER)).isPresent()) {
      Optional.ofNullable(custom.get(TEMPLATE)).ifPresent(template -> StreamSupport.stream(
              Spliterators.spliteratorUnknownSize(template.fieldNames(), Spliterator.ORDERED), false)
          .forEach(taskName -> {
            JsonNode taskJsonNode = template.get(taskName);
            if (!taskJsonNode.has(DEPENDS_ON)) {
              ((ObjectNode) taskJsonNode).replace(DEPENDS_ON, JsonNodeFactory.instance.arrayNode());
            }
            ((ObjectNode) taskJsonNode).replace(DISABLE,
                JsonNodeFactory.instance.booleanNode(true));
            taskList.put(taskName, taskJsonNode);
          }));
      StreamSupport.stream(
              Spliterators.spliteratorUnknownSize(tasks.fieldNames(), Spliterator.ORDERED), false)
          .forEach(taskName -> {
            JsonNode taskJsonNode = tasks.get(taskName);
            ((ObjectNode) taskJsonNode).replace(DISABLE,
                JsonNodeFactory.instance.booleanNode(false));
            taskList.put(taskName, taskJsonNode);
          });
    } else {
      String delimiter = !Optional.ofNullable(custom.get(DELIMITER)).isPresent() ? ","
          : custom.get(DELIMITER).toString();
      ArrayList<HashMap<String, Object>> taskRenders = this.customizedTransformRenderFormat(
          custom.get(RENDER), delimiter);
      if (Optional.ofNullable(custom.get(TEMPLATE)).isPresent()) {
        JsonNode template = custom.get(TEMPLATE);
        StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(template.fieldNames(), Spliterator.ORDERED), false)
            .forEach(templateTaskName -> {
              JsonNode taskJsonNode = template.get(templateTaskName);
              ((ObjectNode) taskJsonNode).replace(DISABLE,
                  JsonNodeFactory.instance.booleanNode(true));
              if (!taskJsonNode.has(DEPENDS_ON)) {
                ((ObjectNode) taskJsonNode).replace(DEPENDS_ON,
                    JsonNodeFactory.instance.arrayNode());
              }
            });
        String templateString = this.formatTaskJsonNode(template);
        taskList.putAll(this.customizedTransformRenderTask(templateString, taskRenders));
      }
      String tasksString = this.formatTaskJsonNode(tasks);
      taskList.putAll(this.customizedTransformRenderTask(tasksString, taskRenders));
    }
    return taskList;
  }

  private String formatTaskJsonNode(JsonNode tasks) throws Exception {
    return TransferDevUtil.yamlMapper.writeValueAsString(
            TransferDevUtil.yamlMapper.readTree(tasks.toString()))
        .replaceAll("(\\s)(\"\\{\\{)((\\s)*(\\S)+(\\s)*)(}}\")", " {{$3}}");
  }

  private void customizedTasksFormat(JsonNode tasksNode) {
    ArrayNode        dependsOnNode = JsonNodeFactory.instance.arrayNode();
    Iterator<String> tasksIter     = tasksNode.fieldNames();
    while (tasksIter.hasNext()) {
      String   taskFieldName = tasksIter.next();
      JsonNode degree        = tasksNode.get(taskFieldName).get(DEPENDS_ON);
      if (!Optional.ofNullable(degree).isPresent() || degree.isEmpty() || degree.size() == 0) {
        ((ObjectNode) tasksNode.get(taskFieldName)).replace(DEPENDS_ON, dependsOnNode);
      }
    }
  }

  private ArrayList<HashMap<String, Object>> customizedTransformRenderFormat(JsonNode renderNode,
      String delimiter) throws Exception {
    return TransferRenderUtil.render(renderNode, delimiter);
  }

  private LinkedHashMap<String, Object> customizedTransformRenderTask(String tasksTemplate,
      ArrayList<HashMap<String, Object>> taskRenders) throws JsonProcessingException {
    LinkedHashMap<String, Object> taskList = new LinkedHashMap<>();
    for (HashMap<String, Object> taskRender : taskRenders) {
      String   taskListStr = TransferDevUtil.jinjava.render(tasksTemplate, taskRender);
      JsonNode taskNode    = TransferDevUtil.yamlMapper.readTree(taskListStr);
      StreamSupport.stream(
              Spliterators.spliteratorUnknownSize(taskNode.fieldNames(), Spliterator.ORDERED), false)
          .forEach(taskName -> {
            if (!Optional.ofNullable(taskNode.get(taskName).get(DISABLE)).isPresent()) {
              ((ObjectNode) taskNode.get(taskName)).replace(DISABLE,
                  JsonNodeFactory.instance.booleanNode(false));
            }
            taskList.put(taskName, taskNode.get(taskName));
          });
    }
    return taskList;
  }
}
