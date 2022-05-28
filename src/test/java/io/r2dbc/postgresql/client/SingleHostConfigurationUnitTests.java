package io.r2dbc.postgresql.client;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

public class SingleHostConfigurationUnitTests {

    @Test
    void builderNoHostAndSocket() {
        assertThatIllegalArgumentException().isThrownBy(() -> SingleHostConfiguration.builder().build())
            .withMessage("host or socket must not be null");
    }

    @Test
    void builderHostAndSocket() {
        assertThatIllegalArgumentException().isThrownBy(() -> SingleHostConfiguration.builder().host("host").socket("socket").build())
            .withMessageContaining("either host/port or socket");
    }
}
