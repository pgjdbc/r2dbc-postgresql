package io.r2dbc.postgresql.client;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link SSLConfig}.
 */
final class SSLConfigTests {
    @Test
    public void testValidSniHostname(){
        assertThat(SSLConfig.isValidSniHostname("example.com")).isEqualTo(true);
        assertThat(SSLConfig.isValidSniHostname("example://.com")).isEqualTo(false);
        assertThat(SSLConfig.isValidSniHostname("example.com.")).isEqualTo(false);
    }
}
