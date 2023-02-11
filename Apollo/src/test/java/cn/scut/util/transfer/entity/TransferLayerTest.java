package cn.scut.util.transfer.entity;

import static cn.scut.app.util.transfer.constant.TransferConfigs.START;

import cn.scut.app.util.transfer.TransferNavigator;
import cn.scut.app.util.transfer.checker.impl.MetaChecker;
import cn.scut.app.util.transfer.context.TransferContext;
import cn.scut.app.util.transfer.entity.output.TransferParams;
import cn.scut.app.util.transfer.entity.output.TransferWorkflow;
import cn.scut.app.util.transfer.entity.output.TransferYaml;
import cn.scut.app.util.transfer.entity.output.layer.impl.TransferDag;
import cn.scut.app.util.transfer.entity.output.layer.impl.TransferGroup;
import cn.scut.app.util.transfer.entity.output.layer.impl.TransferStage;
import cn.scut.app.util.transfer.entity.output.layer.impl.TransferTask;
import cn.scut.app.util.transfer.navigator.MetaNavigator;
import cn.scut.app.util.transfer.transformer.train.KrylovTrainTransformer;
import cn.scut.app.util.transfer.utils.TransferDevUtil;
import io.vavr.control.Either;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Logger;
import javax.xml.transform.TransformerException;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

public class TransferLayerTest {

  TransferContext          context;
  HashMap<String, Object> root;
  TransferParams           transferParams;
  TransferDag              transferDag;
  TransferYaml             transferYaml;

  @Before
  public void metaTransformTest() throws Throwable {
    String template =
        FileUtils.readFileToString(
            new File("data/meta.yaml"), Charsets.toCharset("UTF-8"));
    String transTemplate = this.yamlContentStringFormatting(template);
    context = new TransferContext();
    root    = TransferDevUtil.yaml.load(transTemplate);
    this.metaTransformer(root, context);
  }

  @Test
  public void groupTest() throws Throwable {
    KrylovTrainTransformer transformer = new KrylovTrainTransformer();
    TransferGroup transform = transformer.transform(
        root, context);
    List<TransferTask> transferTaskList = new ArrayList<>();
    transform.getAllTasks()
        .forEach(
            task -> transferTaskList.add(new TransferTask(task.getTaskId(), task.getTaskConfig())));
    TransferGroup                  krylovTransferGroup = new TransferGroup("krylov", transferTaskList);
    LinkedHashMap<String, Object> taskMap            = new LinkedHashMap<>();
    krylovTransferGroup.getAllTasks()
        .forEach(task -> taskMap.put(task.getLayerId(), task.getTaskConfig()));
    FileUtils.writeStringToFile(
        new File("data/meta_dynamic_dag.yaml"),
        TransferDevUtil.yamlMapper.writeValueAsString(taskMap), Charsets.toCharset("UTF-8"));
  }

  @Test
  public void stageTest() throws Throwable {
    KrylovTrainTransformer transformer = new KrylovTrainTransformer();
    TransferGroup trainTransferGroup = transformer.transform(
        root, context);
    List<TransferGroup> transferGroupList = new ArrayList<>();
    transferGroupList.add(trainTransferGroup);
    TransferStage trainTransferStage = new TransferStage("train", transferGroupList,
        new HashMap<>());
    trainTransferStage.createRangeTask(
        context.getTransferConfigs().getTransferStagesTasks().get("train").get(START)
            .split("_")[0]);
    LinkedHashMap<String, Object> taskMap = new LinkedHashMap<>();
    trainTransferStage.getAllTasks()
        .forEach(task -> taskMap.put(task.getLayerId(), task.getTaskConfig()));
    FileUtils.writeStringToFile(
        new File("src/main/yaml/pls/pls_meta_group_test.yaml"),
        TransferDevUtil.yamlMapper.writeValueAsString(taskMap), Charsets.toCharset("UTF-8"));
    for (String groupName : trainTransferStage.getGroupName()) {
      assert trainTransferStage.getGroupName().size() == 1;
      assert groupName.equals("train_krylov");
    }
    System.out.println(trainTransferStage.getGroupInfo());
    System.out.println(trainTransferStage.getGroupDependencies());
  }

  @Test
  public void dagTest() throws Throwable {
    Set<String>               yamlKeys        = root.keySet();
    List<TransferStage>        stageList       = new ArrayList<>();
    Map<String, List<String>> stageOrder      = new HashMap<>();
    String                    preDagStageName = null;
    for (String dagName : context.getTransferConfigs().getDagOrder()) {
      String curDagStageName = context.getTransferConfigs().getLevelOneField(dagName);
      if (yamlKeys.contains(curDagStageName)) {
        Logger.getGlobal().info("Stage start: " + dagName.toUpperCase());
        Either<Throwable, TransferStage> navigatorEither =
            checkAndNavigate(root, context, dagName);
        TransferStage transferStage = navigatorEither.getOrElseThrow(
            () -> new TransformerException(navigatorEither.getLeft().getMessage()));
        stageList.add(transferStage);
        if (Optional.ofNullable(preDagStageName).isPresent()) {
          stageOrder.put(preDagStageName, Collections.singletonList(curDagStageName));
        }
        preDagStageName = curDagStageName;
      }
    }
    this.transferDag = new TransferDag(
        context.getTransferParams().getDagId(), stageList, stageOrder);
    System.out.println(transferDag.getStageName());
    System.out.println(transferDag.getStageDependencies());
    transferDag.getStageInfo().forEach((key, value) -> {
      System.out.println(key);
      System.out.println(value.getGroupDependencies());
    });
    transferYaml = new TransferYaml(transferParams, transferDag);
  }


  @Test
  public void yamlTest() throws Throwable {
    dagTest();
    TransferWorkflow transferWorkflow = transferYaml.getTransferWorkflow();
    System.out.println("stages:\n" + transferWorkflow.getStages());
    System.out.println("groups:\n" + transferWorkflow.getGroups());
    System.out.println("groups in stage:\n" + transferWorkflow.getStage2group());
  }

  private void metaTransformer(HashMap<String, Object> root,
      TransferContext context) throws Throwable {
    Either<Throwable, Boolean> checkEither = new MetaChecker().check(root, context);
    checkEither.getOrElseThrow(checkEither::getLeft);
    MetaNavigator metaNavigator = new MetaNavigator();
    Either<Throwable, TransferParams> transformerEither =
        metaNavigator.navigate(root, context);
    this.transferParams = transformerEither.getOrElseThrow(
        () -> new TransformerException(transformerEither.getLeft().getMessage()));
    context.setTransferParams(this.transferParams);
  }

  private String yamlContentStringFormatting(String template) {
    return template.replaceAll(
        "(\\s)(\\{\\{(\\s)*(\\S)+(\\s)*}})", " \"$2\"");
  }

  private Either<Throwable, TransferStage> checkAndNavigate(
      HashMap<String, Object> root, TransferContext context,
      String signKey) throws Throwable {
    Either<Throwable, Boolean> checkEither = context.getTransferConfigs()
        .getTransferChecker(signKey).check(root, context);
    checkEither.getOrElseThrow(checkEither::getLeft);
    TransferNavigator navigator = new TransferNavigator(context, signKey);
    return navigator.navigate(context, root);
  }

}
