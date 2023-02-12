package cn.scut.app.entity.response;

import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PageResultResponse<T> {

  private Long    total;
  private Integer page;
  private Integer size;
  private List<T> pageResult = new ArrayList<>();
}
