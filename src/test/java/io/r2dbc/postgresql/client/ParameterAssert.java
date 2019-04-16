/*
 * Copyright 2017-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.postgresql.client;

import io.netty.buffer.ByteBuf;
import io.r2dbc.postgresql.message.Format;
import org.assertj.core.api.AbstractAssert;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Objects;

public final class ParameterAssert extends AbstractAssert<ParameterAssert, Parameter> {

    private ParameterAssert(Parameter actual) {
        super(actual, ParameterAssert.class);
    }

    public static ParameterAssert assertThat(Parameter actual) {
        return new ParameterAssert(actual);
    }

    public ParameterAssert hasFormat(Format expected) {
        isNotNull();

        if (this.actual.getFormat() != expected) {
            failWithMessage("Expected parameter's format to be <%s> but was <%s>", expected, this.actual);
        }

        return this;
    }

    public ParameterAssert hasType(Integer expected) {
        isNotNull();

        if (!Objects.equals(this.actual.getType(), expected)) {
            failWithMessage("Expected parameter's type to be <%d> but was <%d>", expected, this.actual);
        }

        return this;
    }

    public ParameterAssert hasValue(ByteBuf... expected) {
        isNotNull();

        Flux<ByteBuf> value = this.actual.getValue();
        if (value == null) {

            if (expected.length != 0) {
                failWithMessage("Expected parameter's value to be null but was not");
            }

            return this;
        }

        value
            .as(StepVerifier::create)
            .expectNext(expected)
            .verifyComplete();

        return this;
    }

}
