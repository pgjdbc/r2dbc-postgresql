package io.r2dbc.postgresql;

import reactor.util.Logger;
import reactor.util.Loggers;

/**
 * Binding logger to log parameter bindings
 */
final class BindingLogger {

    private static final Logger BINDING_LOGGER = Loggers.getLogger("io.r2dbc.postgresql.PARAM");

    static void logBinding(int index, String bindValue) {
        BINDING_LOGGER.debug("Parameter {} value bound: {}", index, bindValue);
    }

    static void logBinding(String name, String bindValue) {
        BINDING_LOGGER.debug("Parameter {} value bound: {}", name, bindValue);
    }
}
