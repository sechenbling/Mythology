package cn.scut.app.util.transfer.transformer.train;

import cn.scut.app.util.transfer.context.TransferContext;
import cn.scut.app.util.transfer.entity.output.layer.impl.TransferGroup;
import cn.scut.app.util.transfer.transformer.Transformer;
import java.util.HashMap;

public class UnknownTrainTransformer implements Transformer {

  public TransferGroup transform(HashMap<String, Object> root,
      TransferContext context) {
    throw new RuntimeException("Unknown training tasks, pls check your train type");
  }
}
