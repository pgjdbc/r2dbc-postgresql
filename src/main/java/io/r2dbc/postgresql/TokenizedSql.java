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

package io.r2dbc.postgresql;

import reactor.util.annotation.Nullable;

import java.util.*;

class TokenizedSql {

    private final String sql;

    private final List<TokenizedStatement> statements;

    private final int statementCount;

    private final TokenizedParameter parameters;

    public TokenizedSql(String sql, List<TokenizedStatement> statements) {
        this.sql = sql;
        this.statements = statements;
        this.statementCount = statements.size();
        this.parameters = getTokenizedParameter(statements);
    }

    List<TokenizedStatement> getStatements() {
        return this.statements;
    }

    public int getStatementCount() {
        return this.statementCount;
    }

    public int getParameterCount() {
        return this.parameters.getParameterCount();
    }

    public ParameterIndex getParameterIndexes(String name) {
        return parameters.getParameterIndexes(name);
    }

    public String getSql() {
        return sql;
    }

    private static TokenizedParameter getTokenizedParameter(List<TokenizedStatement> statements) {
        Map<String, ParameterIndex> parameterIndexMap = new HashMap<>();
        int count = 0;
        for (TokenizedStatement statement : statements) {
            for (Token token : statement.tokens) {
                if (token.type != TokenType.PARAMETER) {
                    continue;
                }
                ParameterIndex value = parameterIndexMap.get(token.value);
                if (value == null) {
                    parameterIndexMap.put(token.value, new ParameterIndex(count));
                } else {
                    value.push(count);
                }
                count++;
            }
        }
        if (count > 0) {
            return new TokenizedParameter(parameterIndexMap, count);
        }
        return new TokenizedParameter(Collections.EMPTY_MAP, count);
    }

    public boolean hasDefaultTokenValue(String... tokenValues) {
        for (TokenizedStatement statement : this.statements) {
            for (Token token : statement.getTokens()) {
                if (token.getType() == TokenType.DEFAULT) {
                    for (String value : tokenValues) {
                        if (token.getValue().equalsIgnoreCase(value)) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    static class Token {

        private final TokenType type;

        private final String value;

        public Token(TokenType type, String value) {
            this.type = type;
            this.value = value;
        }

        public TokenType getType() {
            return this.type;
        }

        public String getValue() {
            return this.value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Token token = (Token) o;

            if (this.type != token.type) {
                return false;
            }
            return this.value.equals(token.value);
        }

        @Override
        public int hashCode() {
            int result = this.type.hashCode();
            result = 31 * result + this.value.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "Token{" +
                "type=" + this.type +
                ", value=" + this.value +
                '}';
        }

    }

    static class TokenizedStatement {

        private final String sql;

        private final List<Token> tokens;

        public TokenizedStatement(String sql, List<Token> tokens) {
            this.tokens = tokens;
            this.sql = sql;
        }

        public String getSql() {
            return this.sql;
        }

        public List<Token> getTokens() {
            return this.tokens;
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

            if (!this.sql.equals(that.sql)) {
                return false;
            }
            return this.tokens.equals(that.tokens);
        }

        @Override
        public int hashCode() {
            int result = this.sql.hashCode();
            result = 31 * result + this.tokens.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "Statement{" +
                "tokens=" + this.tokens +
                '}';
        }

    }

    static class TokenizedParameter {

        private final Map<String, ParameterIndex> namedParameterIndexes;
        private final int parameterCount;

        TokenizedParameter(Map<String, ParameterIndex> namedParameterIndexes, int parameterCount) {
            this.namedParameterIndexes = namedParameterIndexes;
            this.parameterCount = parameterCount;
        }

        public ParameterIndex getParameterIndexes(String name) {
            ParameterIndex index = namedParameterIndexes.get(name);
            if (index == null) {
                throw new NoSuchElementException(String.format("\"%s\" is not a valid identifier", name));
            }
            return index;
        }

        public int getParameterCount() {
            return this.parameterCount;
        }
    }

    static class ParameterIndex {

        private final int first;

        private int[] values;

        private int size = 1;

        ParameterIndex(int first) {
            this.first = first;
        }

        void push(int value) {
            if (this.values == null) {
                this.values = new int[]{this.first, value, 0, 0};
                this.size = 2;
            } else {
                int i = this.size++;
                if (i >= this.values.length) {
                    int[] data = new int[this.values.length << 1];
                    System.arraycopy(this.values, 0, data, 0, this.values.length);
                    this.values = data;
                }
                this.values[i] = value;
            }
        }

        public int getFirst() {
            return first;
        }

        @Nullable
        public int[] getValues() {
            return values;
        }

        @Override
        public String toString() {
            if (this.values == null) {
                return Integer.toString(this.first);
            } else {
                StringBuilder builder = (new StringBuilder()).append('[').append(this.values[0]);
                for(int i = 1; i < this.size; ++i) {
                    builder.append(", ").append(this.values[i]);
                }
                return builder.append(']').toString();
            }
        }

    }

    enum TokenType {
        DEFAULT,
        STRING_CONSTANT,
        COMMENT,
        PARAMETER,
        QUOTED_IDENTIFIER,
        STATEMENT_END,
        SPECIAL_OR_OPERATOR
    }

}
