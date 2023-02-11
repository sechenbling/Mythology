package cn.scut.app.util.transfer.navigator;

import cn.scut.app.util.transfer.context.TransferContext;
import cn.scut.app.util.transfer.entity.output.layer.impl.TransferStage;
import cn.scut.app.util.transfer.transformer.Transformer;
import io.vavr.control.Either;
import java.util.HashMap;

public interface Navigator {

  Either<Throwable, TransferStage> navigate(HashMap<String, Object> root,
      TransferContext context);

  default void setNavigatorStrategies(HashMap<String, Transformer> strategies) {

  }
}
