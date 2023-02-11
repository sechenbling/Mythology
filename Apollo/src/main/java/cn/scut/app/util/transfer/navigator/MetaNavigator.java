package cn.scut.app.util.transfer.navigator;

import cn.scut.app.util.transfer.context.TransferContext;
import cn.scut.app.util.transfer.entity.output.TransferParams;
import cn.scut.app.util.transfer.transformer.meta.DagParamsTransformer;
import cn.scut.app.util.transfer.transformer.meta.MetaTransformer;
import io.vavr.control.Either;
import io.vavr.control.Try;
import java.util.HashMap;

public class MetaNavigator {

  public Either<Throwable, TransferParams> navigate(HashMap<String, Object> root,
      TransferContext context) {
    return Try.of(() -> {
      TransferParams transferParams = this.metaTransform(root, context);
      transferParams.addTasksParams(this.dagParamsTransform(root, context).getTasksParamsMap());
      return transferParams;
    }).toEither();
  }

  private TransferParams metaTransform(HashMap<String, Object> root,
      TransferContext context) {
    MetaTransformer metaTransform = new MetaTransformer();
    return metaTransform.transform(root, context);
  }

  private TransferParams dagParamsTransform(HashMap<String, Object> root,
      TransferContext context) {
    DagParamsTransformer dagParamsTransform = new DagParamsTransformer();
    return dagParamsTransform.transform(root, context);
  }
}
