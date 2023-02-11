package cn.scut.app.util.transfer.navigator.impl;

import cn.scut.app.util.transfer.context.TransferContext;
import cn.scut.app.util.transfer.entity.output.layer.impl.TransferStage;
import cn.scut.app.util.transfer.navigator.Navigator;
import cn.scut.app.util.transfer.transformer.Transformer;
import io.vavr.control.Either;
import java.util.HashMap;
import javax.xml.transform.TransformerException;

public class UnknownNavigator implements Navigator {

  @Override
  public Either<Throwable, TransferStage> navigate(HashMap<String, Object> root,
      TransferContext context) {
    return Either.left(new TransformerException("Unknown transform type!"));
  }

  @Override
  public void setNavigatorStrategies(HashMap<String, Transformer> strategies) {
  }

}
