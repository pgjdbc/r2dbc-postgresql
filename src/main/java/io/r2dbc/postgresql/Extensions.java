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

package io.r2dbc.postgresql;

import io.r2dbc.postgresql.extension.Extension;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ServiceLoader;
import java.util.function.Consumer;

/**
 * Utility to load and hold {@link Extension}s.
 */
final class Extensions {

    private static final Logger logger = Loggers.getLogger(Extensions.class);

    private final List<Extension> extensions;

    private Extensions(List<Extension> extensions) {
        this.extensions = extensions;
    }

    /**
     * Create a new {@link Extensions} object.
     *
     * @param extensions the extensions to hold
     * @return a new {@link Extensions} object
     */
    static Extensions from(Collection<Extension> extensions) {
        return new Extensions(new ArrayList<>(extensions));
    }

    /**
     * Autodetect extensions using {@link ServiceLoader} mechanism.
     *
     * @return the detected {@link Extension}s
     */
    static Extensions autodetect() {
        logger.debug("Discovering Extensions using ServiceLoader");

        ServiceLoader<Extension> extensions = AccessController.doPrivileged((PrivilegedAction<ServiceLoader<Extension>>) () -> ServiceLoader.load(Extension.class,
            Extensions.class.getClassLoader()));

        List<Extension> discovered = new ArrayList<>();
        for (Extension extension : extensions) {

            logger.debug("Registering extension {}", extension.getClass().getName());
            discovered.add(extension);
        }

        return new Extensions(discovered);
    }

    /**
     * Apply {@link Consumer consumer} for each {@link Extension} of the requested type {@code extensionType}
     * until all {@link Extension extensions} have been processed or the action throws an exception.
     * Actions are performed in the order of iteration, if that order is specified.
     * Exceptions thrown by the action are relayed to the caller.
     *
     * @param extensionType the extension type to filter for
     * @param consumer      the {@link Consumer} to notify for each instance of {@code extensionType}
     * @param <T>           extension type
     */
    <T extends Extension> void forEach(Class<T> extensionType, Consumer<T> consumer) {
        for (Extension extension : this.extensions) {
            if (extensionType.isInstance(extension)) {
                consumer.accept(extensionType.cast(extension));
            }
        }
    }

    /**
     * Create a new {@link Extensions} object that contains all extensions from this and {@code other}.
     *
     * @param other the additional {@link Extensions} to add
     * @return a new {@link Extensions} object that contains all extensions from this and {@code other}
     */
    Extensions mergeWith(Extensions other) {
        List<Extension> extensions = new ArrayList<>(size() + other.size());
        extensions.addAll(this.extensions);
        extensions.addAll(other.extensions);
        return new Extensions(extensions);
    }

    /**
     * Returns the number of {@link Extension}s.
     *
     * @return the number of {@link Extension}s
     */
    public int size() {
        return this.extensions.size();
    }

}
