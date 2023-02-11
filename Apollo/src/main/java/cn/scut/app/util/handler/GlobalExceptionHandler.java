package cn.scut.app.util.handler;

import cn.scut.app.exception.CustomException;
import cn.scut.app.response.CommonResponse;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.NoSuchElementException;
import javax.servlet.http.HttpServletRequest;
import javax.validation.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.multipart.MaxUploadSizeExceededException;

@ControllerAdvice
public class GlobalExceptionHandler {

  private static final Logger logger = LoggerFactory.getLogger(GlobalExceptionHandler.class);

  @ExceptionHandler(RuntimeException.class)
  @ResponseBody
  @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
  public ResponseEntity<CommonResponse> runtimeExceptionHandler(HttpServletRequest request,
      RuntimeException e) {
    logger.error(e.toString());
    return CommonResponse.internalError(e.getMessage()).toResponseEntity();
  }

  @ExceptionHandler(IOException.class)
  @ResponseBody
  @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
  public ResponseEntity<CommonResponse> ioExceptionHandler(HttpServletRequest request,
      IOException e) {
    logger.error(e.toString());
    logger.error(request.toString());
    return CommonResponse.internalError(e.getMessage()).toResponseEntity();
  }

  @ExceptionHandler(DataIntegrityViolationException.class)
  @ResponseBody
  @ResponseStatus(HttpStatus.BAD_REQUEST)
  public ResponseEntity<CommonResponse> sqlInsertExceptionHandler(HttpServletRequest request,
      DataIntegrityViolationException e) {
    e.printStackTrace();
    String errorMessage = e.getMessage();
    if (e.getCause() != null && e.getCause()
        .getCause() instanceof SQLIntegrityConstraintViolationException) {
      errorMessage = e.getCause().getCause().getMessage();
    }
    logger.error(request.toString());
    return CommonResponse.badRequest(errorMessage).toResponseEntity();
  }

  @ExceptionHandler(ObjectOptimisticLockingFailureException.class)
  @ResponseBody
  @ResponseStatus(HttpStatus.BAD_REQUEST)
  public ResponseEntity<CommonResponse> optimisticLockVersionConflictExceptionHandler(
      HttpServletRequest request,
      ObjectOptimisticLockingFailureException e) {
    logger.error(e.toString());
    logger.error(request.toString());
    return CommonResponse.badRequest(
            "Update operation meets optimistic lock version conflict. Please try again.")
        .toResponseEntity();
  }

  @ExceptionHandler(Exception.class)
  @ResponseBody
  @ResponseStatus(HttpStatus.BAD_REQUEST)
  public ResponseEntity<CommonResponse> exceptionHandler(HttpServletRequest request, Exception e) {
    logger.error(e.toString());
    logger.error(request.toString());
    return CommonResponse.badRequest(e.getMessage()).toResponseEntity();
  }

  @ExceptionHandler(HttpMessageNotReadableException.class)
  @ResponseBody
  @ResponseStatus(HttpStatus.BAD_REQUEST)
  public ResponseEntity<CommonResponse> requestDeserializationExceptionHandler(
      HttpServletRequest request,
      HttpMessageNotReadableException e) {
    logger.error(e.toString());
    logger.error(request.toString());
    return CommonResponse.badRequest(e.getMessage()).toResponseEntity();
  }

  @ExceptionHandler({MethodArgumentNotValidException.class})
  @ResponseStatus(HttpStatus.BAD_REQUEST)
  @ResponseBody
  public ResponseEntity<CommonResponse> handleMethodArgumentNotValidException(
      HttpServletRequest request, MethodArgumentNotValidException e) {
    logger.error(e.toString());
    logger.error(request.toString());
    return CommonResponse.badRequest(e.getMessage()).toResponseEntity();
  }

  @ExceptionHandler({ConstraintViolationException.class})
  @ResponseStatus(HttpStatus.BAD_REQUEST)
  @ResponseBody
  public ResponseEntity<CommonResponse> handleConstraintViolationException(
      HttpServletRequest request, ConstraintViolationException e) {
    logger.error(e.toString());
    logger.error(request.toString());
    return CommonResponse.badRequest(e.getMessage()).toResponseEntity();
  }

  @ExceptionHandler(MaxUploadSizeExceededException.class)
  @ResponseStatus(HttpStatus.BAD_REQUEST)
  @ResponseBody
  public ResponseEntity<CommonResponse> handleMaxSizeException(HttpServletRequest request,
      MaxUploadSizeExceededException e) {
    logger.error(e.toString());
    logger.error(request.toString());
    return CommonResponse.badRequest(e.getMessage()).toResponseEntity();
  }

  @ExceptionHandler(CustomException.class)
  @ResponseStatus(HttpStatus.BAD_REQUEST)
  @ResponseBody
  public ResponseEntity<CommonResponse> handleCustomException(HttpServletRequest request,
      CustomException e) {
    logger.error(e.getMessage());
    logger.error(request.toString());
    return CommonResponse.badRequest(e.getMessage()).toResponseEntity();
  }


  @ExceptionHandler(NoSuchElementException.class)
  @ResponseStatus(HttpStatus.NOT_FOUND)
  @ResponseBody
  public ResponseEntity<CommonResponse> handleForbiddenException(HttpServletRequest request,
      NoSuchElementException e) {
    logger.error(e.getMessage());
    logger.error(request.toString());
    return CommonResponse.notFound(e.getMessage()).toResponseEntity();
  }

  @ExceptionHandler(SQLException.class)
  @ResponseStatus(HttpStatus.BAD_REQUEST)
  @ResponseBody
  public ResponseEntity<CommonResponse> handleForbiddenException(HttpServletRequest request,
      SQLException e) {
    logger.error(e.getMessage());
    logger.error(request.toString());
    return CommonResponse.badRequest(e.getMessage()).toResponseEntity();
  }
}

