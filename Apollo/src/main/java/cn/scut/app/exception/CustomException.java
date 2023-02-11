package cn.scut.app.exception;

public class CustomException extends RuntimeException {

  public CustomException(Throwable cause) {
    super(cause);
  }

  public CustomException(String errorMessage) {
    super(errorMessage);
  }
}
