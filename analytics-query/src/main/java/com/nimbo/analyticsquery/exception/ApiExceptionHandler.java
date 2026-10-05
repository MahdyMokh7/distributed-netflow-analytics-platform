package com.nimbo.analyticsquery.exception;

import com.nimbo.analyticsquery.dto.response.ErrorResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.ConstraintViolationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidation(
            MethodArgumentNotValidException exception,
            HttpServletRequest request) {

        Map<String, String> violations = new LinkedHashMap<>();

        exception.getBindingResult().getFieldErrors().forEach(
                error -> violations.putIfAbsent(error.getField(), error.getDefaultMessage()));

        return response(
                HttpStatus.BAD_REQUEST,
                "Request validation failed",
                request,
                violations);
    }

    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<ErrorResponse> handleConstraintViolation(
            ConstraintViolationException exception,
            HttpServletRequest request) {

        return response(
                HttpStatus.BAD_REQUEST,
                "Request validation failed",
                request,
                Map.of());
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ErrorResponse> handleMalformedJson(
            HttpMessageNotReadableException exception,
            HttpServletRequest request) {

        return response(
                HttpStatus.BAD_REQUEST,
                "Malformed JSON request",
                request,
                Map.of());
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleUnexpected(
            Exception exception,
            HttpServletRequest request) {

        log.error("Unexpected request failure", exception);

        return response(
                HttpStatus.INTERNAL_SERVER_ERROR,
                "Unexpected internal server error",
                request,
                Map.of());
    }

    private ResponseEntity<ErrorResponse> response(
            HttpStatus status,
            String message,
            HttpServletRequest request,
            Map<String, String> violations) {

        ErrorResponse body = new ErrorResponse(
                Instant.now(),
                status.value(),
                status.getReasonPhrase(),
                message,
                request.getRequestURI(),
                violations);

        return ResponseEntity.status(status).body(body);
    }
}
