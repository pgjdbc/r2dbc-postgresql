package io.r2dbc.postgresql;

import io.r2dbc.postgresql.api.PostgresqlConnection;
import io.r2dbc.postgresql.api.PostgresqlResult;
import io.r2dbc.spi.ColumnMetadata;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcOperations;
import static org.assertj.core.api.Assertions.assertThat;
import reactor.test.StepVerifier;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class ArrayAggIntegrationTests extends AbstractIntegrationTests {


  @Test
  void shouldReadArrayAgg() {

    JdbcOperations jdbcOperations = SERVER.getJdbcOperations();
    jdbcOperations.execute("DROP TABLE IF EXISTS name_table;");
    jdbcOperations.execute("CREATE TABLE name_table (n name)");

    PostgresqlConnection connection = this.connectionFactory.create().block();

    connection.createStatement("INSERT INTO name_table (n) VALUES('hello'), ('world'), (null)")
        .execute()
        .flatMap(PostgresqlResult::getRowsUpdated)
        .as(StepVerifier::create)
        .expectNextCount(1)
        .verifyComplete();

    connection.createStatement("select array_agg(n) as names from name_table")
        .execute()
        .flatMap(it -> it.map((row, rowMetadata) -> row.get("names")))
        .as(StepVerifier::create)
        .assertNext(arr->{
          assertThat((Object[])arr).containsExactly(new Object[]{"hello", "world", null});
        })
        .verifyComplete();

    connection.close().block();
  }
}
