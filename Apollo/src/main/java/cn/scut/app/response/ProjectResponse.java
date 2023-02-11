package cn.scut.app.response;

import cn.scut.app.entity.AbstractBaseEntity;
import java.util.HashSet;
import java.util.Set;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProjectResponse extends AbstractBaseEntity {

  @NotNull
  private Long         projectId;
  @NotNull
  private String       projectName;
  private String       description;
  private String       version;
  @NotNull
  private String      domain;
  private Set<String> owners = new HashSet<>();
}
