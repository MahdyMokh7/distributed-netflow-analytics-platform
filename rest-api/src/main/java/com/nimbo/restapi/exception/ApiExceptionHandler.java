package com.nimbo.restapi.exception;

import com.nimbo.restapi.controller.BandwidthController;
import com.nimbo.restapi.controller.ConnectionController;
import com.nimbo.restapi.controller.ProtocolController;
import com.nimbo.restapi.controller.TrafficController;
import com.nimbo.restapi.dto.response.ApiResponseDto;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import jakarta.validation.ConstraintViolationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice(
    assignableTypes = {
        TrafficController.class,
        ConnectionController.class,
        BandwidthController.class,
        ProtocolController.class
    })
@Slf4j
public class ApiExceptionHandler {

  @ExceptionHandler(ResourceNotFoundException.class)
  public ResponseEntity<ApiResponseDto<Void>> handleNotFound(ResourceNotFoundException exception) {
    return response(HttpStatus.NOT_FOUND, exception.getMessage());
  }

  @ExceptionHandler({
      InvalidTimeRangeException.class,
      IllegalArgumentException.class,
      ConstraintViolationException.class,
      MethodArgumentNotValidException.class
  })
  public ResponseEntity<ApiResponseDto<Void>> handleBadRequest(Exception exception) {
    return response(HttpStatus.BAD_REQUEST, exception.getMessage());
  }

  @ExceptionHandler({DataAccessException.class, CallNotPermittedException.class})
  public ResponseEntity<ApiResponseDto<Void>> handleDatabaseUnavailable(Exception exception) {
    log.error("PostgreSQL query unavailable", exception);
    return response(
        HttpStatus.SERVICE_UNAVAILABLE, "PostgreSQL query service is temporarily unavailable");
  }

  private ResponseEntity<ApiResponseDto<Void>> response(HttpStatus status, String message) {
    return ResponseEntity.status(status).body(ApiResponseDto.error(message));
  }
}
