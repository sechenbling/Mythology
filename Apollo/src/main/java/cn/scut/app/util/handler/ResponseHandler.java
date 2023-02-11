package cn.scut.app.util.handler;

import cn.scut.app.response.CommonResponse;
import org.springframework.http.ResponseEntity;

public class ResponseHandler {
  private ResponseHandler() {
  }

  public static ResponseEntity<CommonResponse> handle(CommonResponse response) {
    return response.toResponseEntity();
  }

  public static ResponseEntity<CommonResponse> handle(Object result) {
    return CommonResponse.success(result).toResponseEntity();
  }
}
