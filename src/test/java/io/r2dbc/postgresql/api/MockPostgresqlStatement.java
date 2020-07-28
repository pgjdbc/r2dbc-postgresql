package io.r2dbc.postgresql.api;

import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;

public class MockPostgresqlStatement implements PostgresqlStatement {

    private final Flux<PostgresqlResult> results;

    public MockPostgresqlStatement(Flux<PostgresqlResult> results) {
        this.results = results;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public PostgresqlStatement add() {
        return this;
    }

    @Override
    public PostgresqlStatement bind(String identifier, Object value) {
        return this;
    }

    @Override
    public PostgresqlStatement bind(int index, Object value) {
        return this;
    }

    @Override
    public PostgresqlStatement bindNull(String identifier, Class<?> type) {
        return this;
    }

    @Override
    public PostgresqlStatement bindNull(int index, Class<?> type) {
        return this;
    }

    @Override
    public Flux<PostgresqlResult> execute() {
        return this.results;
    }

    @Override
    public PostgresqlStatement returnGeneratedValues(String... columns) {
        return this;
    }

    public static final class Builder {

        private final List<PostgresqlResult> results = new ArrayList<>();

        private Builder() {
        }

        public MockPostgresqlStatement build() {
            return new MockPostgresqlStatement(Flux.fromIterable(this.results));
        }

        public Builder result(PostgresqlResult result) {
            this.results.add(result);
            return this;
        }

    }

}
