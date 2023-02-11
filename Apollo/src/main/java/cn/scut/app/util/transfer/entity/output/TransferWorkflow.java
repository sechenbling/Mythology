package cn.scut.app.util.transfer.entity.output;

import cn.scut.app.util.transfer.entity.output.layer.impl.TransferDag;
import cn.scut.app.util.transfer.entity.output.layer.impl.TransferStage;
import cn.scut.app.util.transfer.entity.sql.Group;
import cn.scut.app.util.transfer.entity.sql.Stage;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;

@Getter
public class TransferWorkflow {

  private final String                   projectName;
  private final List<Stage>              stages;
  private final Map<String, List<Group>> stage2group;
  private final List<Group>              groups;

  public TransferWorkflow(String projectName, TransferDag adapterDag) {
    this.projectName = projectName;
    this.stages      = new ArrayList<>();
    this.stage2group = new HashMap<>();
    this.groups      = new ArrayList<>();
    this.adapterLayerAnalysis(adapterDag);
  }

  public void adapterLayerAnalysis(TransferDag adapterDag) {
    adapterDag.getStageName().forEach(stageName -> {
      List<String>  stageDependency = adapterDag.getStageDependencies().get(stageName);
      TransferStage stage           = adapterDag.getStageInfo().get(stageName);
      List<String> stageContainsGroup = new ArrayList<>(
          stage.getGroupName());
      this.stages.add(new Stage(stageName, stageDependency, stageContainsGroup));
      List<Group> groupList = new ArrayList<>();
      stageContainsGroup.forEach(groupName -> {
        List<String> groupDependency = stage.getGroupDependencies().get(groupName);
        Group        group           = new Group(groupName, groupDependency, stageName);
        groupList.add(group);
        groups.add(group);
      });
      stage2group.put(stageName, groupList);
    });
  }
}
