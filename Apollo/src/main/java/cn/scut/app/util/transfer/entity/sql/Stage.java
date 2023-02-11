package cn.scut.app.util.transfer.entity.sql;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Stage {
  private String       stageName;
  private List<String> dependency;
  private List<String> containsGroup;
}
