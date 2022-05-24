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

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

class ParsedSql {

    private final String sql;

    private final List<TokenizedStatement> statements;

    private final int statementCount;

    private final int parameterCount;

    public ParsedSql(String sql, List<TokenizedStatement> statements) {
        this.sql = sql;
        this.statements = statements;
        this.statementCount = statements.size();
        this.parameterCount = getParameterCount(statements);
    }

    List<TokenizedStatement> getStatements() {
        return this.statements;
    }

    public int getStatementCount() {
        return this.statementCount;
    }

    public int getParameterCount() {
        return this.parameterCount;
    }

    public String getSql() {
        return sql;
    }

    private static int getParameterCount(List<TokenizedStatement> statements) {
        int sum = 0;
        for (TokenizedStatement statement : statements){
            sum += statement.getParameterCount();
        }
        return sum;
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

        private final int parameterCount;

        public TokenizedStatement(String sql, List<Token> tokens) {
            this.tokens = tokens;
            this.sql = sql;
            this.parameterCount = readParameterCount(tokens);
        }

        public String getSql() {
            return this.sql;
        }

        public List<Token> getTokens() {
            return this.tokens;
        }

        public int getParameterCount() {
            return this.parameterCount;
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

        private static int readParameterCount(List<Token> tokens) {
            Set<Integer> parameters = new TreeSet<>();

            for (Token token : tokens) {
                if (token.getType() != TokenType.PARAMETER) {
                    continue;
                }
                try {
                    int i = Integer.parseInt(token.getValue().substring(1));
                    parameters.add(i);
                } catch (NumberFormatException | IndexOutOfBoundsException e) {
                    throw new IllegalArgumentException("Illegal parameter token: " + token.getValue());
                }
            }

            int current = 1;
            for (Integer i : parameters) {
                if (i != current) {
                    throw new IllegalArgumentException("Missing parameter number $" + i);
                }
                current++;
            }
            return parameters.size();
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
