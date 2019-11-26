package io.r2dbc.postgresql.client;

import io.r2dbc.postgresql.util.PostgresqlHighAvailabilityClusterExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class HighAvailabilityClusterTest {

    @RegisterExtension
    static final PostgresqlHighAvailabilityClusterExtension SERVERS = new PostgresqlHighAvailabilityClusterExtension();

    @Test
    void testPrimaryAndStandbyStartup() {
        Assertions.assertFalse(SERVERS.getPrimaryJdbc().queryForObject("show transaction_read_only", Boolean.class));
        Assertions.assertTrue(SERVERS.getStandbyJdbc().queryForObject("show transaction_read_only", Boolean.class));
    }
}
