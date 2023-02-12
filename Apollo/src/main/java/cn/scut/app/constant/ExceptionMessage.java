package cn.scut.app.constant;

public class ExceptionMessage {

  public static final String NOT_FOUND              = "not found";
  public static final String NO_PERMISSION          = "has no permission";
  public static final String ALREADY_EXISTS         = "already exists";
  public static final String NOT_MATCH              = "not match";
  public static final String LACK_OF_MODEL_INSTANCE = "model instance number is not enough";
  public static final String DELIMITER              = ",";

  private ExceptionMessage() {
  }

  private static String commonMessage(String target, String message) {
    return String.format("%s %s", target, message);
  }

  public static String notFoundMessage(String... target) {
    return commonMessage(String.join(DELIMITER, target), NOT_FOUND);
  }

  public static String noPermissionMessage(String target) {
    return commonMessage(target, NO_PERMISSION);
  }

  public static String alreadyExistsMessage(String... targets) {
    return commonMessage(String.join(DELIMITER, targets), ALREADY_EXISTS);
  }


  public static String notMatchMessage(String filed, String source, String target) {
    return commonMessage(String.format("%s: %s and %s", filed, source, target), NOT_MATCH);
  }

  public static String lackOfModelInstancesMessage(String target) {
    return commonMessage(target, LACK_OF_MODEL_INSTANCE);
  }
}
