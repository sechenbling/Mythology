package cn.scut.app.util;

import cn.scut.app.util.transfer.TransferNavigator;
import cn.scut.app.util.transfer.checker.impl.MetaChecker;
import cn.scut.app.util.transfer.context.TransferContext;
import cn.scut.app.util.transfer.entity.input.AmmoYaml;
import cn.scut.app.util.transfer.entity.output.TransferParams;
import cn.scut.app.util.transfer.entity.output.TransferYaml;
import cn.scut.app.util.transfer.entity.output.layer.impl.TransferDag;
import cn.scut.app.util.transfer.entity.output.layer.impl.TransferStage;
import cn.scut.app.util.transfer.navigator.MetaNavigator;
import cn.scut.app.util.transfer.utils.TransferDevUtil;
import com.fasterxml.jackson.databind.JsonNode;
import io.vavr.control.Either;
import io.vavr.control.Try;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Logger;
import javax.xml.transform.TransformerException;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;

public class TransferUtil {

  public static Either<Throwable, TransferYaml> transform(AmmoYaml ammoYaml) {
    TransferUtil transferUtil = new TransferUtil();
    return Try.of(() -> transferUtil.yamlTransform(ammoYaml.getAmmoYamlString())).toEither();
  }

  /**
   * Read a yaml file and transform its content to airflow tasks yaml and write to another file.
   *
   * @param reader input yaml file
   * @param writer output yaml file
   */
  public static Either<Throwable, TransferYaml> transform(File reader, File writer) {
    TransferUtil transferUtil = new TransferUtil();
    return Try.of(() -> {
      String       template     = FileUtils.readFileToString(reader, Charsets.toCharset("UTF-8"));
      TransferYaml transferYaml = transferUtil.yamlTransform(template);
      TransferDevUtil.yamlMapper.writeValue(writer, transferYaml.getYamlContentMap());
      return transferYaml;
    }).toEither();
  }

  /**
   * Transform yaml content string to airflow tasks yaml string.
   *
   * @param yamlContent before transform yaml format content string(must be yaml origin content, not
   *                    json or other format)
   * @return yaml format content string after transforming
   */
  public static Either<Throwable, TransferYaml> transform(String yamlContent) {
    TransferUtil transferUtil = new TransferUtil();
    return Try.of(() -> transferUtil.yamlTransform(yamlContent))
        .toEither();
  }

  /**
   * Transform yaml json node to airflow tasks yaml string.
   *
   * @param rootNode a jsonNode that contains meta yaml info
   * @return yaml format content string after transforming
   */
  public static Either<Throwable, TransferYaml> transform(JsonNode rootNode) {
    TransferUtil transferUtil = new TransferUtil();
    return Try.of(() ->
        transferUtil.yamlTransform(
            TransferDevUtil.yamlMapper.writeValueAsString(rootNode))).toEither();
  }

  private TransferYaml yamlTransform(String template) throws Throwable {
    String                  transTemplate = this.yamlContentStringFormatting(template);
    TransferContext         context       = new TransferContext();
    HashMap<String, Object> root           = TransferDevUtil.yaml.load(transTemplate);
    TransferParams          transferParams = this.metaTransformer(root, context);
    context.setTransferParams(transferParams);
    TransferDag adapterDag = this.stageTransformer(root, context);
    context.setTransferDag(adapterDag);
    return generateTransferYaml(transferParams, adapterDag);
  }

  /**
   * replace {{}} content in yaml string content to "{{}}" to convert string to map.
   *
   * @param template yaml string content
   * @return yaml string content after formatting
   */
  private String yamlContentStringFormatting(String template) {
    return template.replaceAll(
        "(\\s)(\\{\\{(\\s)*(\\S)+(\\s)*}})", " \"$2\"");
  }

  private TransferParams metaTransformer(HashMap<String, Object> root,
      TransferContext context) throws Throwable {
    Either<Throwable, Boolean> checkEither = new MetaChecker().check(root, context);
    checkEither.getOrElseThrow(checkEither::getLeft);
    MetaNavigator metaNavigator = new MetaNavigator();
    Either<Throwable, TransferParams> transformerEither =
        metaNavigator.navigate(root, context);
    return transformerEither.getOrElseThrow(
        () -> new TransformerException(transformerEither.getLeft().getMessage()));
  }

  private TransferDag stageTransformer(HashMap<String, Object> root,
      TransferContext context) throws Throwable {
    Set<String>               yamlKeys        = root.keySet();
    List<TransferStage>       stageList       = new ArrayList<>();
    Map<String, List<String>> stageOrder      = new HashMap<>();
    String                    preDagStageName = null;
    for (String dagName : context.getTransferConfigs().getDagOrder()) {
      String curDagStageName = context.getTransferConfigs().getLevelOneField(dagName);
      if (yamlKeys.contains(curDagStageName)) {
        Logger.getGlobal().info("Stage start: " + dagName.toUpperCase());
        Either<Throwable, TransferStage> navigatorEither =
            checkAndNavigate(root, context, dagName);
        TransferStage adapterStage = navigatorEither.getOrElseThrow(
            () -> new TransformerException(navigatorEither.getLeft().getMessage()));
        stageList.add(adapterStage);
        if (Optional.ofNullable(preDagStageName).isPresent()) {
          stageOrder.put(preDagStageName, Collections.singletonList(curDagStageName));
        }
        preDagStageName = curDagStageName;
        Logger.getGlobal().info("Stage end: " + dagName.toUpperCase());
      }
    }
    return new TransferDag(context.getTransferParams().getDagId(), stageList, stageOrder);
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

  private TransferYaml generateTransferYaml(
      TransferParams transferParams,
      TransferDag adapterDag) throws TransformerException {
    if (adapterDag.getAllTasks().size() == 0) {
      throw new TransformerException(
          "The number of tasks after parsing is 0, please check the task configuration");
    }
    return new TransferYaml(transferParams, adapterDag);
  }
}
