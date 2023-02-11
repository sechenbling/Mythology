package cn.scut.app.util.transfer.checker.impl;

import cn.scut.app.util.transfer.checker.Checker;
import cn.scut.app.util.transfer.context.TransferContext;
import io.vavr.control.Either;
import io.vavr.control.Try;
import java.util.HashMap;
import java.util.logging.Logger;

public class UnknownChecker implements Checker {

  @Override
  public Either<Throwable, Boolean> check(HashMap<String, Object> root, TransferContext context) {
    return Try.of(() -> {
      Logger.getGlobal().info("fields or stage has no checker, ignore the checking process");
      return true;
    }).toEither();
  }
}
