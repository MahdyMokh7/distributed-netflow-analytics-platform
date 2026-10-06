package com.nimbo.cubemaker.discovery;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class NetflowFileTimestampParser {

  private static final Pattern FILE_NAME_PATTERN =
      Pattern.compile(
          "netflow-p\\d{2,}-(\\d{4}-\\d{2}-\\d{2}T\\d{2}-\\d{2}-\\d{2}-\\d{3}Z)-.*\\.parquet");
  private static final DateTimeFormatter FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss-SSS'Z'").withZone(ZoneOffset.UTC);

  private NetflowFileTimestampParser() {}

  public static Optional<Instant> parse(String fileName) {
    Matcher matcher = FILE_NAME_PATTERN.matcher(fileName);
    if (!matcher.matches()) {
      return Optional.empty();
    }
    try {
      return Optional.of(Instant.from(FORMATTER.parse(matcher.group(1))));
    } catch (DateTimeParseException exception) {
      return Optional.empty();
    }
  }
}
