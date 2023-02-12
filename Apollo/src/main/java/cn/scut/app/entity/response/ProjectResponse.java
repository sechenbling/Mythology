package cn.scut.app.entity.response;

import cn.scut.app.entity.sql.AbstractBaseEntity;
import java.util.HashSet;
import java.util.Set;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@EqualsAndHashCode(callSuper = true)
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProjectResponse extends AbstractBaseEntity {

  @NotNull
  private Long        projectId;
  @NotNull
  private String      projectName;
  private String      description;
  private String      version;
  @NotNull
  private String      domain;
  private Set<String> owners = new HashSet<>();
}
