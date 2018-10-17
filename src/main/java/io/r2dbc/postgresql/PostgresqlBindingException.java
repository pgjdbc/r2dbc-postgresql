package io.r2dbc.postgresql;

public class PostgresqlBindingException extends RuntimeException {
  public PostgresqlBindingException(String msg) {
    super(msg);
  }
}
