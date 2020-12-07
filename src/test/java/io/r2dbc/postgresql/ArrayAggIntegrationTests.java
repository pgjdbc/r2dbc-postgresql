package io.r2dbc.postgresql;

import io.r2dbc.postgresql.api.PostgresqlConnection;
import io.r2dbc.postgresql.api.PostgresqlResult;
import io.r2dbc.postgresql.codec.Json;
import io.r2dbc.postgresql.util.PostgresqlServerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.jdbc.core.JdbcOperations;
import reactor.test.StepVerifier;

public class ArrayAggIntegrationTests {
  @RegisterExtension
  static final PostgresqlServerExtension SERVER = new PostgresqlServerExtension();

  private final PostgresqlConnectionConfiguration configuration = PostgresqlConnectionConfiguration.builder()
      .database(SERVER.getDatabase())
      .host(SERVER.getHost())
      .port(SERVER.getPort())
      .password(SERVER.getPassword())
      .username(SERVER.getUsername())
      .forceBinary(true)
      .build();

  private final PostgresqlConnectionFactory connectionFactory = new PostgresqlConnectionFactory(this.configuration);

  @Test
  void shouldReadArrayAgg() {

    JdbcOperations jdbcOperations = SERVER.getJdbcOperations();
    jdbcOperations.execute("DROP TABLE IF EXISTS my_table;");
    jdbcOperations.execute("CREATE TABLE my_table (t text)");

    PostgresqlConnection connection = this.connectionFactory.create().block();

    connection.createStatement("INSERT INTO my_table (t) VALUES('hello'), ('world'), (null)")
        .execute()
        .flatMap(PostgresqlResult::getRowsUpdated)
        .as(StepVerifier::create)
        .expectNextCount(1)
        .verifyComplete();

    connection.createStatement("SELECT array_agg(t) FROM my_table")
        .execute()
        .flatMap(it -> it.map((row, rowMetadata) -> row.get("array_agg")))
        .as(StepVerifier::create)
        .expectNext(new Object[]{"hello", "world", null})
        .verifyComplete();

    connection.close().block();
  }
}
