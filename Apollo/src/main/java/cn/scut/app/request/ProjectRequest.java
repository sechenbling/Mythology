package cn.scut.app.request;

import java.util.HashSet;
import java.util.Set;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import lombok.Data;

@Data
public class ProjectRequest {

  @NotNull
  private String       domain;
  private String       description;
  private String       version;
  @NotNull
  @NotBlank
  private String      createBy;
  @NotEmpty
  private Set<String> owners = new HashSet<>();
}

