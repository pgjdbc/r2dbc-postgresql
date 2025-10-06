/*
 * Copyright 2025 the original author or authors.
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

import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.net.InetSocketAddress;

/**
 * Extension for a disposable server that does not reply, only accepting connections and closing these immediately.
 * The extension injects a {@link InetSocketAddress} that points to the disposable server.
 * <p>
 * Example usage:
 *
 * <pre class="code">
 * public class ExampleTest {
 *
 *     &#064;Test
 *     public void shouldDoSomething(@Disposable InetSocketAddress address) {
 *         // ... connect to address
 *     }
 * }
 * </pre>
 *
 * @author Mark Paluch
 */
@Target({ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@ExtendWith({DisposableServerExtension.class})
public @interface Disposable {

}
