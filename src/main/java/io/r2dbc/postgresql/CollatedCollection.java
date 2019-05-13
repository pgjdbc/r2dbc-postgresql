/*
 * Copyright 2017-2019 the original author or authors.
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

import io.r2dbc.postgresql.util.Assert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

final class CollatedCollection<T> extends ArrayList<T> {

    private final Set<T> contains;

    CollatedCollection(List<T> names) {
        super(Assert.requireNonNull(names, "names must not be null"));

        this.contains = new TreeSet<>(Collator.DEFAULT);
        this.contains.addAll(names);
    }

    @Override
    public boolean contains(Object o) {
        return this.contains.contains(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return this.contains.containsAll(c);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CollatedCollection)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        CollatedCollection<?> that = (CollatedCollection<?>) o;
        return this.contains.equals(that.contains);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.contains);
    }

}
