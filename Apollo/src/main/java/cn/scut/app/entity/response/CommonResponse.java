package cn.scut.app.entity.response;

import lombok.Builder;
import lombok.Data;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

@Data
@Builder
public class CommonResponse {

  private Integer status;
  private String  errorMessage;
  private Object  result;

  public static CommonResponse success(Object result) {
    return CommonResponse.builder()
        .status(HttpStatus.OK.value())
        .result(result)
        .build();
  }

  public static CommonResponse badRequest(String errorMessage) {
    return error(HttpStatus.BAD_REQUEST.value(), errorMessage);
  }

  public static CommonResponse internalError(String errorMessage) {
    return error(HttpStatus.INTERNAL_SERVER_ERROR.value(), errorMessage);
  }

  public static CommonResponse notFound(String errorMessage) {
    return error(HttpStatus.NOT_FOUND.value(), errorMessage);
  }

  public static CommonResponse forbidden(String errorMessage) {
    return error(HttpStatus.FORBIDDEN.value(), errorMessage);
  }

  private static CommonResponse error(Integer status, String errorMessage) {
    return CommonResponse.builder()
        .status(status)
        .errorMessage(errorMessage)
        .build();
  }

  public final ResponseEntity<CommonResponse> toResponseEntity() {
    return ResponseEntity.status(HttpStatus.valueOf(this.status)).body(this);
  }
}
