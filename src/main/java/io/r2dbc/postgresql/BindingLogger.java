package io.r2dbc.postgresql;

import io.r2dbc.postgresql.client.ConnectionContext;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.util.Objects;

/**
 * Binding logger to log parameter bindings
 *
 * @since 0.8.6
 */
final class BindingLogger {

    private static final Logger BINDING_LOGGER = Loggers.getLogger("io.r2dbc.postgresql.PARAM");

    private static final boolean LOGGING_ENABLED = BINDING_LOGGER.isDebugEnabled();

    static void logBind(ConnectionContext connectionContext, int index, Object bindValue) {

        if (LOGGING_ENABLED) {
            BINDING_LOGGER.debug(connectionContext.getMessage("Bind parameter [{}] to: {}"), index, Objects.toString(bindValue));
        }
    }

    static void logBind(ConnectionContext connectionContext, String name, Object bindValue) {

        if (LOGGING_ENABLED) {
            BINDING_LOGGER.debug(connectionContext.getMessage("Bind parameter [{}] to: {}"), name, Objects.toString(bindValue));
        }
    }

    static void logBindNull(ConnectionContext connectionContext, int index, Class<?> type) {

        if (LOGGING_ENABLED) {
            BINDING_LOGGER.debug(connectionContext.getMessage("Bind parameter [{}] to null, type: {}"), index, type.getName());
        }
    }

    static void logBindNull(ConnectionContext connectionContext, String name, Class<?> type) {

        if (LOGGING_ENABLED) {
            BINDING_LOGGER.debug(connectionContext.getMessage("Bind parameter [{}] to null, type: {}"), name, type.getName());
        }
    }

}
