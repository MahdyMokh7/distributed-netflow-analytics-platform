package com.nimbo.restapi.validation;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import java.net.InetAddress;
import java.net.UnknownHostException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IpAddressValidator implements ConstraintValidator<ValidIpAddress, String> {

  @Override
  public boolean isValid(String value, ConstraintValidatorContext context) {
    if (value == null || value.isBlank()) {
      return true;
    }

    try {
      InetAddress.getByName(value);
      return true;
    } catch (UnknownHostException e) {
      log.debug("Invalid IP address: {}", value);
      return false;
    }
  }
}
