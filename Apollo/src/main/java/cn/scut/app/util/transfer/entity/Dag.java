package cn.scut.app.util.transfer.entity;

import static cn.scut.app.util.transfer.constant.TransferConfigs.END_TASK_SUFFIX;
import static cn.scut.app.util.transfer.constant.TransferConfigs.START_TASK_SUFFIX;
import static cn.scut.app.util.transfer.constant.TransferTaskFields.DEPENDS_ON;
import static cn.scut.app.util.transfer.constant.TransferTaskFields.GROUP_ID;
import static cn.scut.app.util.transfer.constant.TransferTaskFields.SETTINGS;
import static cn.scut.app.util.transfer.constant.TransferTaskFields.TASK_TYPE;

import cn.scut.app.util.transfer.entity.output.layer.TransferLayer;
import cn.scut.app.util.transfer.entity.output.layer.impl.TransferTask;
import cn.scut.app.util.transfer.utils.TransferDevUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.vavr.control.Try;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;

public class Dag<T extends TransferLayer> {

  List<T>                   layerList;
  Map<String, T>            layerMap        = new HashMap<>();
  Set<String>               layerNodes      = new HashSet<>();
  Map<String, List<String>> layerEdges      = new HashMap<>();
  List<ArrayList<String>>   taskEdges       = new ArrayList<>();
  Set<String>               taskNodes       = new HashSet<>();
  Set<String>               headTaskNames   = new HashSet<>();
  Set<String>               tailTaskNames   = new HashSet<>();
  List<TransferTask>        headList        = new ArrayList<>();
  List<TransferTask>        tailList        = new ArrayList<>();
  List<TransferTask>        adapterTaskList = new ArrayList<>();
  boolean                   dagFlag;

  public Dag(List<T> layerList) {
    layerList.forEach(layer -> {
      layerMap.put(layer.getLayerId(), layer);
      layerNodes.add(layer.getLayerId());
    });
    layerEdges = new HashMap<>();
    List<TransferTask> tasks = new ArrayList<>();
    this.layerList = layerList;
    layerList.forEach(layer -> tasks.addAll(layer.getAllTasks()));
    layerDagAnalysis(tasks);
    dagFlag = layerDagConstruct();
    if (tailTaskNames.isEmpty()) {
      tailTaskNames.addAll(headTaskNames);
    }
    layerDagTaskLoad();
  }

  public Dag(List<T> layerList, Map<String, List<String>> layerOrder) {
    layerList.forEach(layer -> layerMap.put(layer.getLayerId(), layer));
    layerList.forEach(layer -> {
      String layerId = layer.getLayerId();
      this.layerNodes.add(layerId);
      this.layerEdges.put(layerId, layerOrder.getOrDefault(layerId, null));
    });
    this.layerList = layerList;
    layerDagAnalysis(layerTypologyConnect(layerTopologyOrder()));
    dagFlag = layerDagConstruct();
    if (tailTaskNames.isEmpty()) {
      tailTaskNames.addAll(headTaskNames);
    }
    layerDagTaskLoad();
  }

  public List<String> layerTopologyOrder() {
    List<String>         order    = new ArrayList<>();
    Map<String, Integer> inDegree = new HashMap<>();
    layerNodes.forEach(layerNode -> inDegree.put(layerNode, 0));
    layerEdges.forEach((key, value) ->
        Optional.ofNullable(value)
            .ifPresent(v -> v.forEach(
                e -> inDegree.put(e, inDegree.get(e) + 1))));
    Queue<String> queue = new LinkedList<>();
    inDegree.forEach((key, value) -> {
      if (value == 0) {
        queue.offer(key);
      }
    });
    int nodeNums = 0;
    while (!queue.isEmpty()) {
      nodeNums++;
      String foo = queue.poll();
      order.add(foo);
      Optional.ofNullable(layerEdges.get(foo))
          .ifPresent(
              fo -> fo.forEach(bar -> {
                int inbar = inDegree.get(bar) - 1;
                inDegree.put(bar, inDegree.get(bar) - 1);
                if (inbar == 0) {
                  queue.offer(bar);
                }
              }));
    }
    if (layerNodes.size() != nodeNums) {
      throw new IllegalStateException("DAG TOPOLOGY INIT FAILED");
    }
    return order;
  }

  public List<TransferTask> layerTypologyConnect(List<String> order) {
    List<TransferTask> newList = new ArrayList<>();
    String             pre     = null;
    for (String cur : order) {
      T curLayer = layerMap.get(cur);
      if (Optional.ofNullable(pre).isPresent()) {
        ArrayNode dependsOn = JsonNodeFactory.instance.arrayNode();
        layerMap.get(pre).getTailTasks()
            .forEach(preTailTask -> dependsOn.add(preTailTask.getTaskId()));
        Set<String> curHeadTaskSet = new HashSet<>();
        curLayer.getHeadTasks()
            .forEach(curHeadTask -> curHeadTaskSet.add(curHeadTask.getTaskId()));
        curLayer.getAllTasks().forEach(curTask -> {
          if (curHeadTaskSet.contains(curTask.getTaskId())) {
            JsonNode taskConfig = Try.of(() -> TransferDevUtil.yamlMapper.readTree(
                TransferDevUtil.yamlMapper.writeValueAsString(curTask.getTaskConfig()))).get();
            ((ObjectNode) taskConfig).replace(DEPENDS_ON, dependsOn);
            TransferTask task = new TransferTask(curTask.getTaskId(), taskConfig);
            newList.add(task);
          } else {
            newList.add(curTask);
          }
        });
      } else {
        newList.addAll(curLayer.getAllTasks());
      }
      pre = curLayer.getLayerId();
    }
    return newList;
  }

  private void layerDagAnalysis(List<TransferTask> list) {
    list.forEach(task -> {
      adapterTaskList.add(task);
      String curTask = task.getLayerId();
      taskNodes.add(curTask);
      List<String> taskDependsOn = task.getDependsOn();
      if (!taskDependsOn.isEmpty()) {
        taskDependsOn.forEach(preTask -> {
          ArrayList<String> edge = new ArrayList<>();
          edge.add(preTask);
          edge.add(curTask);
          taskEdges.add(edge);
        });
      } else {
        headTaskNames.add(curTask);
      }
    });
  }

  private boolean layerDagConstruct() {
    Map<String, Integer> outDegree = new HashMap<>();
    taskNodes.forEach(taskNode -> {
      if (!headTaskNames.contains(taskNode)) {
        outDegree.put(taskNode, 0);
      }
    });
    Map<String, Integer> inDegree = new HashMap<>();
    taskEdges.forEach(taskEdge -> {
      String preNode  = taskEdge.get(0);
      String nextNode = taskEdge.get(1);
      inDegree.put(nextNode, inDegree.getOrDefault(nextNode, 0) + 1);
      outDegree.put(preNode, outDegree.getOrDefault(preNode, 0) + 1);
    });
    taskNodes.forEach(taskNode -> {
      if (!headTaskNames.contains(taskNode) && outDegree.get(taskNode) == 0) {
        tailTaskNames.add(taskNode);
      }
    });
    int           visited = 0;
    Queue<String> queue   = new LinkedList<>(headTaskNames);
    while (!queue.isEmpty()) {
      ++visited;
      String taskNode = queue.poll();
      taskEdges.forEach(taskEdge -> {
        if (taskEdge.get(0).equals(taskNode)) {
          String nextNode = taskEdge.get(1);
          inDegree.put(nextNode, inDegree.get(nextNode) - 1);
          if (inDegree.get(nextNode) == 0) {
            queue.offer(nextNode);
          }
        }
      });
    }
    return visited == taskNodes.size();
  }

  private void layerDagTaskLoad() {
    adapterTaskList.forEach(task -> {
      if (headTaskNames.contains(task.getLayerId())) {
        headList.add(task);
      }
      if (tailTaskNames.contains(task.getLayerId())) {
        tailList.add(task);
      }
    });
  }

  public List<TransferTask> createRangeTask(String id) {
    this.adapterTaskList = createStartTask(id);
    this.adapterTaskList = createEndTask(id);
    this.adapterTaskList = addGroupIdToTasks(id);
    return adapterTaskList;
  }

  private List<TransferTask> createStartTask(String id) {
    ObjectNode        startTaskConfig    = JsonNodeFactory.instance.objectNode();
    startTaskConfig.replace(TASK_TYPE, JsonNodeFactory.instance.textNode("bash"));
    ObjectNode settings = JsonNodeFactory.instance.objectNode();
    settings.replace("bash_command",
        JsonNodeFactory.instance.textNode(
            "echo '== TASK WITH ID " + id + " START =='"));
    startTaskConfig.replace(SETTINGS, settings);
    startTaskConfig.replace(DEPENDS_ON, JsonNodeFactory.instance.arrayNode());
    String             startTaskId        = id + START_TASK_SUFFIX;
    TransferTask       startAdapterTask   = new TransferTask(startTaskId, startTaskConfig);
    List<TransferTask> newAdapterTaskList = new ArrayList<>();
    newAdapterTaskList.add(startAdapterTask);
    ArrayNode dependsOn = JsonNodeFactory.instance.arrayNode();
    dependsOn.add(startTaskId);
    adapterTaskList.forEach(task -> {
      String layerId = task.getLayerId();
      if (this.getHeadTaskNames().contains(layerId)) {
        Try.of(() -> {
          JsonNode taskConfigNode = TransferDevUtil.yamlMapper.readTree(
              TransferDevUtil.yamlMapper.writeValueAsString(task.getTaskConfig()));
          ((ObjectNode) taskConfigNode).replace(DEPENDS_ON, dependsOn);
          newAdapterTaskList.add(new TransferTask(layerId, taskConfigNode));
          return true;
        });
      } else {
        newAdapterTaskList.add(task);
      }
    });
    headTaskNames.clear();
    headTaskNames.add(startTaskId);
    headList.clear();
    headList.add(startAdapterTask);
    return newAdapterTaskList;
  }

  private List<TransferTask> createEndTask(String id) {
    ObjectNode endTaskConfig = JsonNodeFactory.instance.objectNode();
    endTaskConfig.replace(TASK_TYPE, JsonNodeFactory.instance.textNode("bash"));
    ObjectNode settings = JsonNodeFactory.instance.objectNode();
    settings.replace("bash_command",
        JsonNodeFactory.instance.textNode(
            "echo '== TASK WITH ID " + id + " END =='"));
    endTaskConfig.replace(SETTINGS, settings);
    ArrayNode dependsOn = JsonNodeFactory.instance.arrayNode();

    this.getTailTaskNames().forEach(dependsOn::add);
    endTaskConfig.replace(DEPENDS_ON, dependsOn);
    String       endTaskId      = id + END_TASK_SUFFIX;
    TransferTask endAdapterTask = new TransferTask(endTaskId, endTaskConfig);
    this.adapterTaskList.add(endAdapterTask);
    this.tailTaskNames.clear();
    this.tailTaskNames.add(endTaskId);
    this.tailList.clear();
    this.tailList.add(endAdapterTask);
    return adapterTaskList;
  }

  private List<TransferTask> addGroupIdToTasks(String id) {
    List<TransferTask> tasks = new LinkedList<>();
    this.adapterTaskList.forEach(adapterTask -> Try.of(() -> {
      JsonNode taskConfigNode = TransferDevUtil.yamlMapper.readTree(
          TransferDevUtil.yamlMapper.writeValueAsString(adapterTask.getTaskConfig()));
      if (taskConfigNode.has(GROUP_ID)) {
        String groupIdSuffix = taskConfigNode.get(GROUP_ID).asText();
        ((ObjectNode) taskConfigNode).replace(GROUP_ID,
            JsonNodeFactory.instance.textNode(id + "." + groupIdSuffix));
      } else {
        ((ObjectNode) taskConfigNode).replace(GROUP_ID,
            JsonNodeFactory.instance.textNode(id));
      }
      tasks.add(new TransferTask(adapterTask.getTaskId(), taskConfigNode));
      return true;
    }));
    return tasks;
  }

  public Map<String, T> getLayerMap() {
    return layerMap;
  }

  public Set<String> getLayerNodes() {
    return layerNodes;
  }

  public Map<String, List<String>> getLayerEdges() {
    return layerEdges;
  }

  public Set<String> getHeadTaskNames() {
    return headTaskNames;
  }

  public Set<String> getTailTaskNames() {
    return tailTaskNames;
  }

  public List<TransferTask> getHeadList() {
    return headList;
  }

  public List<TransferTask> getTailList() {
    return tailList;
  }

  public List<TransferTask> getTaskList() {
    return adapterTaskList;
  }

  public boolean isDag() {
    return dagFlag;
  }
}
