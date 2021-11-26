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

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PostgresqlSqlLexerTest {

    @Nested
    class SingleStatementTests {

        @Nested
        class SingleTokenTests {

            @Test
            void singleQuotedStringIsTokenized() {
                assertSingleStatementEqualsCompleteToken("'Test'", TokenizedSql.TokenType.STRING_CONSTANT);
            }

            @Test
            void dollarQuotedStringIsTokenized() {
                assertSingleStatementEqualsCompleteToken("$$test$$", TokenizedSql.TokenType.STRING_CONSTANT);
            }

            @Test
            void dollarQuotedTaggedStringIsTokenized() {
                assertSingleStatementEqualsCompleteToken("$a$test$a$", TokenizedSql.TokenType.STRING_CONSTANT);
            }

            @Test
            void quotedIdentifierIsTokenized() {
                assertSingleStatementEqualsCompleteToken("\"test\"", TokenizedSql.TokenType.QUOTED_IDENTIFIER);
            }

            @Test
            void lineCommentIsTokenized() {
                assertSingleStatementEqualsCompleteToken("--test", TokenizedSql.TokenType.COMMENT);
            }

            @Test
            void cStyleCommentIsTokenized() {
                assertSingleStatementEqualsCompleteToken("/*Test*/", TokenizedSql.TokenType.COMMENT);
            }

            @Test
            void nestedCStyleCommentIsTokenizedAsSingleToken() {
                assertSingleStatementEqualsCompleteToken("/*/*Test*/*/", TokenizedSql.TokenType.COMMENT);
            }

            @Test
            void windowsMultiLineCStyleCommentIsTokenizedAsSingleToken() {
                assertSingleStatementEqualsCompleteToken("/*Test\r\n Test*/", TokenizedSql.TokenType.COMMENT);
            }

            @Test
            void unixMultiLineCStyleCommentIsTokenizedAsSingleToken() {
                assertSingleStatementEqualsCompleteToken("/*Test\n Test*/", TokenizedSql.TokenType.COMMENT);
            }

            @Test
            void digitIsTokenizedAsDefaultToken() {
                assertSingleStatementEqualsCompleteToken("1", TokenizedSql.TokenType.DEFAULT);
            }

            @Test
            void alphaIsTokenizedAsDefaultToken() {
                assertSingleStatementEqualsCompleteToken("a", TokenizedSql.TokenType.DEFAULT);
            }

            @Test
            void multipleDefaultTokensAreTokenizedAsSingleDefaultToken() {
                assertSingleStatementEqualsCompleteToken("atest123", TokenizedSql.TokenType.DEFAULT);
            }

            @Test
            void parameterIsTokenized() {
                assertSingleStatementEqualsCompleteToken("$1", TokenizedSql.TokenType.PARAMETER);
            }

            @Test
            void statementEndIsTokenized() {
                assertSingleStatementEqualsCompleteToken(";", TokenizedSql.TokenType.STATEMENT_END);
            }

            void assertSingleStatementEqualsCompleteToken(String sql, TokenizedSql.TokenType token) {
                assertSingleStatementEquals(sql, new TokenizedSql.Token(token, sql));
            }

        }

        @Nested
        class SingleTokenExceptionTests {

            @Test
            void unclosedSingleQuotedStringThrowsIllegalArgumentException() {
                assertThrows(IllegalArgumentException.class, () -> PostgresqlSqlLexer.tokenize("'test"));
            }

            @Test
            void unclosedDollarQuotedStringThrowsIllegalArgumentException() {
                assertThrows(IllegalArgumentException.class, () -> PostgresqlSqlLexer.tokenize("$$test"));
            }

            @Test
            void unclosedTaggedDollarQuotedStringThrowsIllegalArgumentException() {
                assertThrows(IllegalArgumentException.class, () -> PostgresqlSqlLexer.tokenize("$abc$test"));
            }

            @Test
            void unclosedQuotedIdentifierThrowsIllegalArgumentException() {
                assertThrows(IllegalArgumentException.class, () -> PostgresqlSqlLexer.tokenize("\"test"));
            }

            @Test
            void unclosedBlockCommentThrowsIllegalArgumentException() {
                assertThrows(IllegalArgumentException.class, () -> PostgresqlSqlLexer.tokenize("/*test"));
            }

            @Test
            void unclosedNestedBlockCommentThrowsIllegalArgumentException() {
                assertThrows(IllegalArgumentException.class, () -> PostgresqlSqlLexer.tokenize("/*/*test*/"));
            }

            @Test
            void invalidParameterCharacterThrowsIllegalArgumentException() {
                assertThrows(IllegalArgumentException.class, () -> PostgresqlSqlLexer.tokenize("$1test"));
            }

            @Test
            void invalidTaggedDollarQuoteThrowsIllegalArgumentException() {
                assertThrows(IllegalArgumentException.class, () -> PostgresqlSqlLexer.tokenize("$a b$test$a b$"));
            }

            @Test
            void unclosedTaggedDollarQuoteThrowsIllegalArgumentException() {
                assertThrows(IllegalArgumentException.class, () -> PostgresqlSqlLexer.tokenize("$abc"));
            }
        }

        @Nested
        class MultipleTokenTests {

            @Test
            void defaultTokenIsEndedBySpecialCharacter() {
                assertSingleStatementEquals("abc[",
                    new TokenizedSql.Token(TokenizedSql.TokenType.DEFAULT, "abc"),
                    new TokenizedSql.Token(TokenizedSql.TokenType.SPECIAL_OR_OPERATOR, "["));
            }

            @Test
            void defaultTokenIsEndedByOperatorCharacter() {
                assertSingleStatementEquals("abc-",
                    new TokenizedSql.Token(TokenizedSql.TokenType.DEFAULT, "abc"),
                    new TokenizedSql.Token(TokenizedSql.TokenType.SPECIAL_OR_OPERATOR, "-"));
            }

            @Test
            void defaultTokenIsEndedByStatementEndCharacter() {
                assertSingleStatementEquals("abc;",
                    new TokenizedSql.Token(TokenizedSql.TokenType.DEFAULT, "abc"),
                    new TokenizedSql.Token(TokenizedSql.TokenType.STATEMENT_END, ";"));
            }

            @Test
            void defaultTokenIsEndedByQuoteCharacter() {
                assertSingleStatementEquals("abc\"def\"",
                    new TokenizedSql.Token(TokenizedSql.TokenType.DEFAULT, "abc"),
                    new TokenizedSql.Token(TokenizedSql.TokenType.QUOTED_IDENTIFIER, "\"def\""));
            }

            @Test
            void parameterTokenIsEndedByQuoteCharacter() {
                assertSingleStatementEquals("$1+",
                    new TokenizedSql.Token(TokenizedSql.TokenType.PARAMETER, "$1"),
                    new TokenizedSql.Token(TokenizedSql.TokenType.SPECIAL_OR_OPERATOR, "+"));
            }

            @Test
            void parameterIsRecognizedBetweenSpecialCharacters() {
                assertSingleStatementEquals("($1)",
                    new TokenizedSql.Token(TokenizedSql.TokenType.SPECIAL_OR_OPERATOR, "("),
                    new TokenizedSql.Token(TokenizedSql.TokenType.PARAMETER, "$1"),
                    new TokenizedSql.Token(TokenizedSql.TokenType.SPECIAL_OR_OPERATOR, ")")
                );
            }

            @Test
            void lineCommentIsEndedAtNewline() {
                assertSingleStatementEquals("--abc\ndef",
                    new TokenizedSql.Token(TokenizedSql.TokenType.COMMENT, "--abc"),
                    new TokenizedSql.Token(TokenizedSql.TokenType.DEFAULT, "def"));
            }

            @Test
            void multipleOperatorsAreSeparatelyTokenized() {
                assertSingleStatementEquals("**",
                    new TokenizedSql.Token(TokenizedSql.TokenType.SPECIAL_OR_OPERATOR, "*"),
                    new TokenizedSql.Token(TokenizedSql.TokenType.SPECIAL_OR_OPERATOR, "*")
                );
            }

        }

        @Nested
        class AssortedRealStatementTests {

            @Test
            void simpleSelectStatementIsTokenized() {
                assertSingleStatementEquals("SELECT * FROM /* A Comment */ table WHERE \"SELECT\" = $1",
                    new TokenizedSql.Token(TokenizedSql.TokenType.DEFAULT, "SELECT"),
                    new TokenizedSql.Token(TokenizedSql.TokenType.SPECIAL_OR_OPERATOR, "*"),
                    new TokenizedSql.Token(TokenizedSql.TokenType.DEFAULT, "FROM"),
                    new TokenizedSql.Token(TokenizedSql.TokenType.COMMENT, "/* A Comment */"),
                    new TokenizedSql.Token(TokenizedSql.TokenType.DEFAULT, "table"),
                    new TokenizedSql.Token(TokenizedSql.TokenType.DEFAULT, "WHERE"),
                    new TokenizedSql.Token(TokenizedSql.TokenType.QUOTED_IDENTIFIER, "\"SELECT\""),
                    new TokenizedSql.Token(TokenizedSql.TokenType.SPECIAL_OR_OPERATOR, "="),
                    new TokenizedSql.Token(TokenizedSql.TokenType.PARAMETER, "$1")
                );
            }

        }

        void assertSingleStatementEquals(String sql, TokenizedSql.Token... tokens) {
            TokenizedSql tokenizedSql = PostgresqlSqlLexer.tokenize(sql);
            assertEquals(1, tokenizedSql.getStatements().size(), "Parse returned zero or more than 2 statements");
            TokenizedSql.TokenizedStatement statement = tokenizedSql.getStatements().get(0);
            assertEquals(new TokenizedSql.TokenizedStatement(sql, Arrays.asList(tokens)), statement);
        }

    }

    @Nested
    class MultipleStatementTests {

        @Test
        void simpleMultipleStatementIsTokenized() {
            TokenizedSql tokenizedSql = PostgresqlSqlLexer.tokenize("DELETE * FROM X; SELECT 1;");
            List<TokenizedSql.TokenizedStatement> statements = tokenizedSql.getStatements();
            assertEquals(2, statements.size());
            TokenizedSql.TokenizedStatement statementA = statements.get(0);
            TokenizedSql.TokenizedStatement statementB = statements.get(1);

            assertEquals(new TokenizedSql.TokenizedStatement("DELETE * FROM X;",
                    Arrays.asList(
                        new TokenizedSql.Token(TokenizedSql.TokenType.DEFAULT, "DELETE"),
                        new TokenizedSql.Token(TokenizedSql.TokenType.SPECIAL_OR_OPERATOR, "*"),
                        new TokenizedSql.Token(TokenizedSql.TokenType.DEFAULT, "FROM"),
                        new TokenizedSql.Token(TokenizedSql.TokenType.DEFAULT, "X"),
                        new TokenizedSql.Token(TokenizedSql.TokenType.STATEMENT_END, ";")
                    )),
                statementA
            );

            assertEquals(new TokenizedSql.TokenizedStatement("SELECT 1;",
                    Arrays.asList(
                        new TokenizedSql.Token(TokenizedSql.TokenType.DEFAULT, "SELECT"),
                        new TokenizedSql.Token(TokenizedSql.TokenType.DEFAULT, "1"),
                        new TokenizedSql.Token(TokenizedSql.TokenType.STATEMENT_END, ";")
                    )),
                statementB
            );

        }

    }

}
