package cn.scut.app.util.transfer;

import cn.scut.app.util.transfer.context.TransferContext;
import cn.scut.app.util.transfer.entity.output.layer.impl.TransferStage;
import cn.scut.app.util.transfer.navigator.Navigator;
import io.vavr.control.Either;
import java.util.HashMap;

public class TransferNavigator {

  private final Navigator navigator;

  public TransferNavigator(TransferContext context, String signKey) {
    navigator = context.getTransferConfigs().getTransferNavigator(signKey);
    navigator.setNavigatorStrategies(
        context.getTransferConfigs().getNavigatorStrategies(signKey));
  }

  public Either<Throwable, TransferStage> navigate(
      TransferContext context, HashMap<String, Object> root) {
    return navigator.navigate(root, context);
  }
}
