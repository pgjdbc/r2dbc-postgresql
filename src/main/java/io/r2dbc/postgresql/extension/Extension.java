/*
 * Copyright 2019 the original author or authors.
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

package io.r2dbc.postgresql.extension;


/**
 * Marker interface for all extensions.
 *
 * <p>An {@link Extension} can be registered <em>programmatically</em> through
 * {@link io.r2dbc.postgresql.PostgresqlConnectionConfiguration} or <em>automatically</em> using
 * the {@link java.util.ServiceLoader} mechanism.
 * <p>
 * The global extension registration through Java's {@link java.util.ServiceLoader} mechanism, allows third-party extensions to be auto-detected and automatically registered based on
 * what is available in the classpath.
 * <p>
 * Specifically, a custom extension can be registered by supplying its fully qualified class name in a file named {@code io.r2dbc.postgresql.extension.Extension} within the
 * {@code /META-INF/services} directory in its enclosing JAR file.
 *
 * <h3>Configuring Automatic Extension Detection</h3>
 * Auto-detection is enabled by default. To disable it, simply set the {@link io.r2dbc.postgresql.PostgresqlConnectionConfiguration.Builder#autodetectExtensions(boolean)} parameter to
 * {@literal false}.
 *
 * <h3>Constructor Requirements</h3>
 * <p>Extension implementations must have a <em>default constructor</em> if registered via the {@code ServiceLoader}.  When registered through
 * {@link io.r2dbc.postgresql.PostgresqlConnectionConfiguration} the default constructor is not required to be {@code public}.
 * When registered via the {@code ServiceLoader} the default constructor must be {@code public}.
 */
public interface Extension {

}
