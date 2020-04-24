/*
 * Copyright 2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.postgresql.util;

import reactor.util.Logger;

import java.util.function.Supplier;

/**
 * Logging utility to control the log level.
 *
 * @since 0.9
 */
public enum LogLevel {

    /**
     * Logging disabled.
     */
    OFF {
        @Override
        public void log(Logger logger, String msg) {

        }

        @Override
        public void log(Logger logger, String format, Object... arguments) {

        }

        @Override
        public void log(Logger logger, Supplier<String> msg) {

        }
    },

    /**
     * Log messages at {@code DEBUG} level.
     */
    DEBUG {
        @Override
        public void log(Logger logger, String msg) {
            logger.debug(msg);
        }

        @Override
        public void log(Logger logger, String format, Object... arguments) {
            logger.debug(format, arguments);
        }

        @Override
        public void log(Logger logger, Supplier<String> msg) {
            if (logger.isDebugEnabled()) {
                logger.debug(msg.get());
            }
        }
    },

    /**
     * Log messages at {@code INFO} level.
     */
    INFO {
        @Override
        public void log(Logger logger, String msg) {
            logger.info(msg);
        }

        @Override
        public void log(Logger logger, String format, Object... arguments) {
            logger.info(format, arguments);
        }

        @Override
        public void log(Logger logger, Supplier<String> msg) {
            if (logger.isInfoEnabled()) {
                logger.info(msg.get());
            }
        }
    },

    /**
     * Log messages at {@code WARN} level.
     */
    WARN {
        @Override
        public void log(Logger logger, String msg) {
            logger.warn(msg);
        }

        @Override
        public void log(Logger logger, String format, Object... arguments) {
            logger.warn(format, arguments);
        }

        @Override
        public void log(Logger logger, Supplier<String> msg) {
            if (logger.isWarnEnabled()) {
                logger.warn(msg.get());
            }
        }
    },

    /**
     * Log messages at {@code ERROR} level.
     */
    ERROR {
        @Override
        public void log(Logger logger, String msg) {
            logger.error(msg);
        }

        @Override
        public void log(Logger logger, String format, Object... arguments) {
            logger.error(format, arguments);
        }

        @Override
        public void log(Logger logger, Supplier<String> msg) {
            if (logger.isErrorEnabled()) {
                logger.error(msg.get());
            }
        }
    };

    /**
     * Log a message.
     *
     * @param logger the logger to use
     * @param msg    the message string to be logged
     */
    public abstract void log(Logger logger, String msg);

    /**
     * Log a message.
     *
     * @param logger the logger to use
     * @param msg    the supplier creating the message string to be logged
     */
    public abstract void log(Logger logger, Supplier<String> msg);

    /**
     * Log a message  according to the specified format  and arguments.
     * <p>This form avoids superfluous string concatenation when the logger is disabled. However, this variant incurs the hidden
     * (and relatively small) cost of creating an {@code Object[]} before invoking the method, even if this logger is disabled.
     *
     * @param logger    the logger to use
     * @param format    the format string
     * @param arguments a list of 3 or more arguments
     */
    public abstract void log(Logger logger, String format, Object... arguments);
}
