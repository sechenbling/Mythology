package cn.scut.app.util.transfer.transformer.evaluation;

import cn.scut.app.util.transfer.context.TransferContext;
import cn.scut.app.util.transfer.entity.output.layer.impl.TransferGroup;
import cn.scut.app.util.transfer.transformer.Transformer;
import java.util.HashMap;

public class ModelEvalTransformer implements Transformer {

  @Override
  public TransferGroup transform(HashMap<String, Object> root,
      TransferContext context) {
    // TODO: model level customized task and their own metrics health check
    return null;
  }
}
