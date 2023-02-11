package cn.scut.app.util.transfer.entity.sql;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Group {

  private String       groupName;
  private List<String> dependency;
  private String       stageBelongTo;
}
