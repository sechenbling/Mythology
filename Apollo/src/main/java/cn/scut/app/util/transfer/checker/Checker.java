package cn.scut.app.util.transfer.checker;

import cn.scut.app.util.transfer.context.TransferContext;
import io.vavr.control.Either;
import java.util.HashMap;

public interface Checker {
  Either<Throwable, Boolean> check(HashMap<String, Object> root,
      TransferContext context);
}
