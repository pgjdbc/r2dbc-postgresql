/*
 * Copyright 2022 the original author or authors.
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

package io.r2dbc.postgresql.codec;

import io.r2dbc.postgresql.util.TestByteBufAllocator;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.springframework.asm.Type;
import org.springframework.core.io.ClassPathResource;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.MAP;

/**
 * Tests for {@code reflect-config.json} to contain reflection hints for codecs.
 */
public class CodecReflectConfigTests {

    @TestFactory
    Stream<DynamicTest> reflectionConfigContainsArrayCodecTypeHints() {

        DefaultCodecs codecs = new DefaultCodecs(TestByteBufAllocator.TEST);

        return StreamSupport.stream(codecs.spliterator(), false)
            .filter(ArrayCodec.class::isInstance)
            .map(ArrayCodec.class::cast)
            .map(it -> {
                return DynamicTest.dynamicTest(String.format("%s (%s)", it.getComponentType().getName(), it.getClass().getSimpleName()), () -> containsReflectionHint(it.getComponentType()));
            });
    }

    void containsReflectionHint(Class<?> componentType) {

        List<Map<String, Object>> list = readReflectConfig();

        Type type = Type.getType(componentType);
        String lDotName = "[" + type.getDescriptor().replace('/', '.');
        String lSlashName = "[" + type.getDescriptor();
        String javaName = componentType.getName() + "[]";
        assertThat(list).describedAs("reflect-config.json entry for " + javaName).filteredOn(map -> {

            Object name = map.get("name");
            return name.equals(lDotName) || name.equals(lSlashName) || name.equals(javaName);
        }).first(as(MAP)).describedAs("reflect-config.json entry for " + javaName).containsEntry("unsafeAllocated", true);
    }

    @SuppressWarnings("unchecked")
    static List<Map<String, Object>> readReflectConfig() {

        ObjectMapper mapper = new ObjectMapper();
        ClassPathResource resource = new ClassPathResource("META-INF/native-image/org.postgresql/r2dbc-postgresql/reflect-config.json");

        try (InputStream is = resource.getInputStream()) {
            return mapper.readValue(is, List.class);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

}
