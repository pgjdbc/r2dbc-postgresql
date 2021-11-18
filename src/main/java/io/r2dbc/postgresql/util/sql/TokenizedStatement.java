/*
 * Copyright 2021 the original author or authors.
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

package io.r2dbc.postgresql.util.sql;

import java.util.List;
import java.util.stream.Collectors;

public class TokenizedStatement {

    private final String sql;

    private final List<Token> tokens;

    private final int parameterCount;

    public TokenizedStatement(String sql, List<Token> tokens) {
        this.tokens = tokens;
        this.sql = sql;
        this.parameterCount = readParameterCount(tokens);
    }

    public String getSql() {
        return sql;
    }

    public List<Token> getTokens() {
        return tokens;
    }

    public int getParameterCount() {
        return parameterCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TokenizedStatement that = (TokenizedStatement) o;

        if (!sql.equals(that.sql)) {
            return false;
        }
        return tokens.equals(that.tokens);
    }

    @Override
    public int hashCode() {
        int result = sql.hashCode();
        result = 31 * result + tokens.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Statement{" +
            "tokens=" + tokens +
            '}';
    }

    private static int readParameterCount(List<Token> tokens) {
        List<Integer> parameterTokens = tokens.stream()
            .filter(t -> t.getType() == TokenType.PARAMETER)
            .map(t -> {
                try {
                    return Integer.parseInt(t.getValue().substring(1));
                } catch (NumberFormatException | IndexOutOfBoundsException e) {
                    throw new IllegalArgumentException("Illegal parameter token: " + t.getValue());
                }
            })
            .distinct()
            .sorted()
            .collect(Collectors.toList());
        int current = 1;
        for (Integer i : parameterTokens) {
            if (i != current) {
                throw new IllegalArgumentException("Missing parameter number $" + i);
            }
            current++;
        }
        return parameterTokens.size();
    }

}
