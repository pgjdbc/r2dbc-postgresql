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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.Character.isWhitespace;

/**
 * Utility to tokenize Postgres SQL statements.
 *
 * @since 0.9
 */
class PostgresqlSqlLexer {

    private static final char[] SPECIAL_AND_OPERATOR_CHARS = {
        '+', '-', '*', '/', '<', '>', '=', '~', '!', '@', '#', '%', '^', '&', '|', '`', '?',
        '(', ')', '[', ']', ',', ';', ':', '*', '.', '\'', '"'
    };

    static {
        Arrays.sort(SPECIAL_AND_OPERATOR_CHARS);
    }

    public static TokenizedSql tokenize(String sql) {
        List<TokenizedSql.Token> tokens = new ArrayList<>();
        List<TokenizedSql.TokenizedStatement> statements = new ArrayList<>();

        int statementStartIndex = 0;
        int i = 0;
        while (i < sql.length()) {
            char c = sql.charAt(i);
            TokenizedSql.Token token = null;

            if (isWhitespace(c)) {
                i++;
                continue;
            }
            switch (c) {
                case '\'': // "Standard" string constant
                    token = getStandardQuoteToken(sql, i);
                    break;
                case '\"': // Quoted Identifier
                    token = getQuotedIdentifierToken(sql, i);
                    break;
                case '-': // Possible start of double-dash comment
                    if ((i + 1) < sql.length() && sql.charAt(i + 1) == '-') {
                        token = getCommentToLineEndToken(sql, i);
                    }
                    break;
                case '/': // Possible start of c-style comment
                    if ((i + 1) < sql.length() && sql.charAt(i + 1) == '*') {
                        token = getBlockCommentToken(sql, i);
                    }
                    break;
                case '$': // Dollar-quoted constant or parameter
                    token = getParameterOrDollarQuoteToken(sql, i);
                    break;
                case ';':
                    token = new TokenizedSql.Token(TokenizedSql.TokenType.STATEMENT_END, ";");
                    break;
                default:
                    break;
            }
            if (token == null) {
                if (isSpecialOrOperatorChar(c)) {
                    token = new TokenizedSql.Token(TokenizedSql.TokenType.SPECIAL_OR_OPERATOR, Character.toString(c));//getSpecialOrOperatorToken(sql, i);
                } else {
                    token = getDefaultToken(sql, i);
                }
            }

            i += token.getValue().length();

            if (token.getType() == TokenizedSql.TokenType.STATEMENT_END) {

                tokens.add(token);
                statements.add(new TokenizedSql.TokenizedStatement(sql.substring(statementStartIndex, i), tokens));

                tokens = new ArrayList<>();
                statementStartIndex = i + 1;
            } else {
                tokens.add(token);
            }
        }
        // If tokens is not empty, implicit statement end
        if (!tokens.isEmpty()) {
            statements.add(new TokenizedSql.TokenizedStatement(sql.substring(statementStartIndex), tokens));
        }

        return new TokenizedSql(sql, statements);
    }

    private static TokenizedSql.Token getDefaultToken(String sql, int beginIndex) {
        for (int i = beginIndex + 1; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (Character.isWhitespace(c) || isSpecialOrOperatorChar(c)) {
                return new TokenizedSql.Token(TokenizedSql.TokenType.DEFAULT, sql.substring(beginIndex, i));
            }
        }
        return new TokenizedSql.Token(TokenizedSql.TokenType.DEFAULT, sql.substring(beginIndex));
    }

    private static boolean isSpecialOrOperatorChar(char c) {
        return Arrays.binarySearch(SPECIAL_AND_OPERATOR_CHARS, c) >= 0;
    }

    private static TokenizedSql.Token getBlockCommentToken(String sql, int beginIndex) {
        int depth = 1;
        for (int i = beginIndex + 2; i < (sql.length() - 1); i++) {
            String biGraph = sql.substring(i, i + 2);

            if (biGraph.equals("/*")) {
                depth++;
                i++;
            } else if (biGraph.equals("*/")) {
                depth--;
                i++;
            }
            if (depth == 0) {
                return new TokenizedSql.Token(TokenizedSql.TokenType.COMMENT, sql.substring(beginIndex, i + 1));
            }
        }
        throw new IllegalArgumentException("Sql cannot be parsed: unclosed block comment (comment opened at index " + beginIndex + ") in statement: " + sql);
    }

    private static TokenizedSql.Token getCommentToLineEndToken(String sql, int beginIndex) {
        int lineEnding = sql.indexOf('\n', beginIndex);
        if (lineEnding == -1) {
            return new TokenizedSql.Token(TokenizedSql.TokenType.COMMENT, sql.substring(beginIndex));
        } else {
            return new TokenizedSql.Token(TokenizedSql.TokenType.COMMENT, sql.substring(beginIndex, lineEnding));
        }
    }

    private static TokenizedSql.Token getDollarQuoteToken(String sql, String tag, int beginIndex) {
        int nextQuote = sql.indexOf(tag, beginIndex + tag.length());
        if (nextQuote == -1) {
            throw new IllegalArgumentException("Sql cannot be parsed: unclosed quote (quote opened at index " + beginIndex + ") in statement: " + sql);
        } else {
            return new TokenizedSql.Token(TokenizedSql.TokenType.STRING_CONSTANT, sql.substring(beginIndex, nextQuote + tag.length()));
        }
    }

    private static TokenizedSql.Token getParameterToken(String sql, int beginIndex) {
        for (int i = beginIndex + 1; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (isWhitespace(c) || isSpecialOrOperatorChar(c)) {
                return new TokenizedSql.Token(TokenizedSql.TokenType.PARAMETER, sql.substring(beginIndex, i));
            }
            if (!isAsciiDigit(c)) {
                throw new IllegalArgumentException("Sql cannot be parsed: illegal character in parameter or dollar-quote tag: " + c);
            }
        }
        return new TokenizedSql.Token(TokenizedSql.TokenType.PARAMETER, sql.substring(beginIndex));
    }

    private static TokenizedSql.Token getParameterOrDollarQuoteToken(String sql, int beginIndex) {
        char firstChar = sql.charAt(beginIndex + 1);
        if (firstChar == '$') {
            return getDollarQuoteToken(sql, "$$", beginIndex);
        } else if (isAsciiDigit(firstChar)) {
            return getParameterToken(sql, beginIndex);
        } else {
            for (int i = beginIndex + 1; i < sql.length(); i++) {
                char c = sql.charAt(i);
                if (c == '$') {
                    return getDollarQuoteToken(sql, sql.substring(beginIndex, i + 1), beginIndex);
                }
                if (!(isAsciiLetter(c) || c == '_' || isAsciiDigit(c))) {
                    return new TokenizedSql.Token(TokenizedSql.TokenType.PARAMETER, sql.substring(beginIndex, i));
                }
            }
            throw new IllegalArgumentException("Sql cannot be parsed: unclosed dollar-quote tag(quote opened at index " + beginIndex + ") in statement: " + sql);
        }
    }

    private static TokenizedSql.Token getStandardQuoteToken(String sql, int beginIndex) {
        int nextQuote = sql.indexOf('\'', beginIndex + 1);
        if (nextQuote == -1) {
            throw new IllegalArgumentException("Sql cannot be parsed: unclosed quote (quote opened at index " + beginIndex + ") in statement: " + sql);
        } else {
            return new TokenizedSql.Token(TokenizedSql.TokenType.STRING_CONSTANT, sql.substring(beginIndex, nextQuote + 1));
        }
    }

    private static TokenizedSql.Token getQuotedIdentifierToken(String sql, int beginIndex) {
        int nextQuote = sql.indexOf('\"', beginIndex + 1);
        if (nextQuote == -1) {
            throw new IllegalArgumentException("Sql cannot be parsed: unclosed quoted identifier (identifier opened at index " + beginIndex + ") in statement: " + sql);
        } else {
            return new TokenizedSql.Token(TokenizedSql.TokenType.QUOTED_IDENTIFIER, sql.substring(beginIndex, nextQuote + 1));
        }
    }

    private static boolean isAsciiLetter(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
    }

    private static boolean isAsciiDigit(char c){
        return c >= '0' && c <= '9';
    }
}
