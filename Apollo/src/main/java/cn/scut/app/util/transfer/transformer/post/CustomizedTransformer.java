package cn.scut.app.util.transfer.transformer.post;

import cn.scut.app.util.transfer.context.TransferContext;
import cn.scut.app.util.transfer.entity.output.layer.impl.TransferGroup;
import cn.scut.app.util.transfer.transformer.Transformer;
import java.util.HashMap;

public class CustomizedTransformer implements Transformer {

  public TransferGroup transform(HashMap<String, Object> root,
      TransferContext context) {
    throw new RuntimeException("Customized Transformer");
  }
}
