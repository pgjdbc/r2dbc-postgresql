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
class PostgresqlSqlParser {

    private static final char[] SPECIAL_AND_OPERATOR_CHARS = {
        '+', '-', '*', '/', '<', '>', '=', '~', '!', '@', '#', '%', '^', '&', '|', '`', '?',
        '(', ')', '[', ']', ',', ';', ':', '*', '.', '\'', '"'
    };

    static {
        Arrays.sort(SPECIAL_AND_OPERATOR_CHARS);
    }

    private static List<ParsedSql.Token> tokenize(String sql) {
        List<ParsedSql.Token> tokens = new ArrayList<>();
        int i = 0;
        while (i < sql.length()) {
            char c = sql.charAt(i);
            ParsedSql.Token token = null;

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
                    token = new ParsedSql.Token(ParsedSql.TokenType.STATEMENT_END, ";");
                    break;
                default:
                    break;
            }
            if (token == null) {
                if (isSpecialOrOperatorChar(c)) {
                    token = new ParsedSql.Token(ParsedSql.TokenType.SPECIAL_OR_OPERATOR, Character.toString(c));//getSpecialOrOperatorToken(sql, i);
                } else {
                    token = getDefaultToken(sql, i);
                }
            }

            i += token.getValue().length();
            tokens.add(token);
        }
        return tokens;
    }

    public static ParsedSql parse(String sql) {
        List<ParsedSql.Token> tokens = tokenize(sql);
        List<ParsedSql.Statement> statements = new ArrayList<>();
        List<Boolean> functionBodyList = new ArrayList<>();

        List<ParsedSql.Token> currentStatementTokens = new ArrayList<>();
        for (int i = 0; i < tokens.size(); i++) {
            ParsedSql.Token current = tokens.get(i);
            currentStatementTokens.add(current);

            if (current.getType() == ParsedSql.TokenType.DEFAULT) {
                String currentValue = current.getValue();

                if (currentValue.equalsIgnoreCase("BEGIN")) {
                    if (i + 1 < tokens.size() && tokens.get(i + 1).getValue().equalsIgnoreCase("ATOMIC")) {
                        functionBodyList.add(true);
                    } else {
                        functionBodyList.add(false);
                    }
                } else if (currentValue.equalsIgnoreCase("END") && !functionBodyList.isEmpty()) {
                    functionBodyList.remove(functionBodyList.size() - 1);
                }
            } else if (current.getType().equals(ParsedSql.TokenType.STATEMENT_END)) {
                boolean inFunctionBody = false;

                for (boolean b : functionBodyList) {
                    inFunctionBody |= b;
                }
                if (!inFunctionBody) {
                    statements.add(new ParsedSql.Statement(currentStatementTokens));
                    currentStatementTokens = new ArrayList<>();
                }
            }
        }

        if (!currentStatementTokens.isEmpty()) {
            statements.add(new ParsedSql.Statement(currentStatementTokens));
        }

        return new ParsedSql(sql, statements);
    }

    private static ParsedSql.Token getDefaultToken(String sql, int beginIndex) {
        for (int i = beginIndex + 1; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (Character.isWhitespace(c) || isSpecialOrOperatorChar(c)) {
                return new ParsedSql.Token(ParsedSql.TokenType.DEFAULT, sql.substring(beginIndex, i));
            }
        }
        return new ParsedSql.Token(ParsedSql.TokenType.DEFAULT, sql.substring(beginIndex));
    }

    private static boolean isSpecialOrOperatorChar(char c) {
        return Arrays.binarySearch(SPECIAL_AND_OPERATOR_CHARS, c) >= 0;
    }

    private static ParsedSql.Token getBlockCommentToken(String sql, int beginIndex) {
        int depth = 1;
        for (int i = beginIndex + 2; i < (sql.length() - 1); i++) {
            char c1 = sql.charAt(i);
            char c2 = sql.charAt(i + 1);
            if (c1 == '/' && c2 == '*') {
                depth++;
                i++;
            } else if (c1 == '*' && c2 == '/') {
                depth--;
                i++;
            }
            if (depth == 0) {
                return new ParsedSql.Token(ParsedSql.TokenType.COMMENT, sql.substring(beginIndex, i + 1));
            }
        }
        throw new IllegalArgumentException("Sql cannot be parsed: unclosed block comment (comment opened at index " + beginIndex + ") in statement: " + sql);
    }

    private static ParsedSql.Token getCommentToLineEndToken(String sql, int beginIndex) {
        int lineEnding = sql.indexOf('\n', beginIndex);
        if (lineEnding == -1) {
            return new ParsedSql.Token(ParsedSql.TokenType.COMMENT, sql.substring(beginIndex));
        } else {
            return new ParsedSql.Token(ParsedSql.TokenType.COMMENT, sql.substring(beginIndex, lineEnding));
        }
    }

    private static ParsedSql.Token getDollarQuoteToken(String sql, String tag, int beginIndex) {
        int nextQuote = sql.indexOf(tag, beginIndex + tag.length());
        if (nextQuote == -1) {
            throw new IllegalArgumentException("Sql cannot be parsed: unclosed quote (quote opened at index " + beginIndex + ") in statement: " + sql);
        } else {
            return new ParsedSql.Token(ParsedSql.TokenType.STRING_CONSTANT, sql.substring(beginIndex, nextQuote + tag.length()));
        }
    }

    private static ParsedSql.Token getParameterToken(String sql, int beginIndex) {
        for (int i = beginIndex + 1; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (isWhitespace(c) || isSpecialOrOperatorChar(c)) {
                return new ParsedSql.Token(ParsedSql.TokenType.PARAMETER, sql.substring(beginIndex, i));
            }
            if (!isAsciiDigit(c)) {
                throw new IllegalArgumentException("Sql cannot be parsed: illegal character in parameter or dollar-quote tag: " + c);
            }
        }
        return new ParsedSql.Token(ParsedSql.TokenType.PARAMETER, sql.substring(beginIndex));
    }

    private static ParsedSql.Token getParameterOrDollarQuoteToken(String sql, int beginIndex) {
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
                    throw new IllegalArgumentException("Sql cannot be parsed: illegal character in dollar-quote tag (quote opened at index " + beginIndex + ") in statement: " + sql);
                }
            }
            throw new IllegalArgumentException("Sql cannot be parsed: unclosed dollar-quote tag(quote opened at index " + beginIndex + ") in statement: " + sql);
        }
    }

    private static ParsedSql.Token getStandardQuoteToken(String sql, int beginIndex) {
        int nextQuote = sql.indexOf('\'', beginIndex + 1);
        if (nextQuote == -1) {
            throw new IllegalArgumentException("Sql cannot be parsed: unclosed quote (quote opened at index " + beginIndex + ") in statement: " + sql);
        } else {
            return new ParsedSql.Token(ParsedSql.TokenType.STRING_CONSTANT, sql.substring(beginIndex, nextQuote + 1));
        }
    }

    private static ParsedSql.Token getQuotedIdentifierToken(String sql, int beginIndex) {
        int nextQuote = sql.indexOf('\"', beginIndex + 1);
        if (nextQuote == -1) {
            throw new IllegalArgumentException("Sql cannot be parsed: unclosed quoted identifier (identifier opened at index " + beginIndex + ") in statement: " + sql);
        } else {
            return new ParsedSql.Token(ParsedSql.TokenType.QUOTED_IDENTIFIER, sql.substring(beginIndex, nextQuote + 1));
        }
    }

    private static boolean isAsciiLetter(char c) {
        char lower = Character.toLowerCase(c);
        return lower >= 'a' && lower <= 'z';
    }

    private static boolean isAsciiDigit(char c) {
        return c >= '0' && c <= '9';
    }

}
