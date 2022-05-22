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
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PostgresqlSqlParserTest {

    @Nested
    class SingleStatementTests {

        @Nested
        class SingleTokenTests {

            @Test
            void singleQuotedStringIsTokenized() {
                assertSingleStatementEqualsCompleteToken("'Test'", ParsedSql.TokenType.STRING_CONSTANT);
            }

            @Test
            void dollarQuotedStringIsTokenized() {
                assertSingleStatementEqualsCompleteToken("$$test$$", ParsedSql.TokenType.STRING_CONSTANT);
            }

            @Test
            void dollarQuotedTaggedStringIsTokenized() {
                assertSingleStatementEqualsCompleteToken("$a$test$a$", ParsedSql.TokenType.STRING_CONSTANT);
            }

            @Test
            void quotedIdentifierIsTokenized() {
                assertSingleStatementEqualsCompleteToken("\"test\"", ParsedSql.TokenType.QUOTED_IDENTIFIER);
            }

            @Test
            void lineCommentIsTokenized() {
                assertSingleStatementEqualsCompleteToken("--test", ParsedSql.TokenType.COMMENT);
            }

            @Test
            void cStyleCommentIsTokenized() {
                assertSingleStatementEqualsCompleteToken("/*Test*/", ParsedSql.TokenType.COMMENT);
                assertSingleStatementEqualsCompleteToken("/**/", ParsedSql.TokenType.COMMENT);
                assertSingleStatementEqualsCompleteToken("/*T*/", ParsedSql.TokenType.COMMENT);
            }

            @Test
            void nestedCStyleCommentIsTokenizedAsSingleToken() {
                assertSingleStatementEqualsCompleteToken("/*/*Test*/*/", ParsedSql.TokenType.COMMENT);
            }

            @Test
            void windowsMultiLineCStyleCommentIsTokenizedAsSingleToken() {
                assertSingleStatementEqualsCompleteToken("/*Test\r\n Test*/", ParsedSql.TokenType.COMMENT);
            }

            @Test
            void unixMultiLineCStyleCommentIsTokenizedAsSingleToken() {
                assertSingleStatementEqualsCompleteToken("/*Test\n Test*/", ParsedSql.TokenType.COMMENT);
            }

            @Test
            void digitIsTokenizedAsDefaultToken() {
                assertSingleStatementEqualsCompleteToken("1", ParsedSql.TokenType.DEFAULT);
            }

            @Test
            void alphaIsTokenizedAsDefaultToken() {
                assertSingleStatementEqualsCompleteToken("a", ParsedSql.TokenType.DEFAULT);
            }

            @Test
            void multipleDefaultTokensAreTokenizedAsSingleDefaultToken() {
                assertSingleStatementEqualsCompleteToken("atest123", ParsedSql.TokenType.DEFAULT);
            }

            @Test
            void parameterIsTokenized() {
                assertSingleStatementEqualsCompleteToken("$1", ParsedSql.TokenType.PARAMETER);
            }

            @Test
            void statementEndIsTokenized() {
                assertSingleStatementEqualsCompleteToken(";", ParsedSql.TokenType.STATEMENT_END);
            }

            void assertSingleStatementEqualsCompleteToken(String sql, ParsedSql.TokenType token) {
                assertSingleStatementEquals(sql, new ParsedSql.Token(token, sql));
            }

        }

        @Nested
        class SingleTokenExceptionTests {

            @Test
            void unclosedSingleQuotedStringThrowsIllegalArgumentException() {
                assertThrows(IllegalArgumentException.class, () -> PostgresqlSqlParser.parse("'test"));
            }

            @Test
            void unclosedDollarQuotedStringThrowsIllegalArgumentException() {
                assertThrows(IllegalArgumentException.class, () -> PostgresqlSqlParser.parse("$$test"));
            }

            @Test
            void unclosedTaggedDollarQuotedStringThrowsIllegalArgumentException() {
                assertThrows(IllegalArgumentException.class, () -> PostgresqlSqlParser.parse("$abc$test"));
            }

            @Test
            void unclosedQuotedIdentifierThrowsIllegalArgumentException() {
                assertThrows(IllegalArgumentException.class, () -> PostgresqlSqlParser.parse("\"test"));
            }

            @Test
            void unclosedBlockCommentThrowsIllegalArgumentException() {
                assertThrows(IllegalArgumentException.class, () -> PostgresqlSqlParser.parse("/*test"));
            }

            @Test
            void unclosedNestedBlockCommentThrowsIllegalArgumentException() {
                assertThrows(IllegalArgumentException.class, () -> PostgresqlSqlParser.parse("/*/*test*/"));
            }

            @Test
            void invalidParameterCharacterThrowsIllegalArgumentException() {
                assertThrows(IllegalArgumentException.class, () -> PostgresqlSqlParser.parse("$1test"));
            }

            @Test
            void invalidTaggedDollarQuoteThrowsIllegalArgumentException() {
                assertThrows(IllegalArgumentException.class, () -> PostgresqlSqlParser.parse("$a b$test$a b$"));
            }

            @Test
            void unclosedTaggedDollarQuoteThrowsIllegalArgumentException() {
                assertThrows(IllegalArgumentException.class, () -> PostgresqlSqlParser.parse("$abc"));
            }

        }

        @Nested
        class MultipleTokenTests {

            @Test
            void defaultTokenIsEndedBySpecialCharacter() {
                assertSingleStatementEquals("abc[",
                    new ParsedSql.Token(ParsedSql.TokenType.DEFAULT, "abc"),
                    new ParsedSql.Token(ParsedSql.TokenType.SPECIAL_OR_OPERATOR, "["));
            }

            @Test
            void defaultTokenIsEndedByOperatorCharacter() {
                assertSingleStatementEquals("abc-",
                    new ParsedSql.Token(ParsedSql.TokenType.DEFAULT, "abc"),
                    new ParsedSql.Token(ParsedSql.TokenType.SPECIAL_OR_OPERATOR, "-"));
            }

            @Test
            void defaultTokenIsEndedByStatementEndCharacter() {
                assertSingleStatementEquals("abc;",
                    new ParsedSql.Token(ParsedSql.TokenType.DEFAULT, "abc"),
                    new ParsedSql.Token(ParsedSql.TokenType.STATEMENT_END, ";"));
            }

            @Test
            void defaultTokenIsEndedByQuoteCharacter() {
                assertSingleStatementEquals("abc\"def\"",
                    new ParsedSql.Token(ParsedSql.TokenType.DEFAULT, "abc"),
                    new ParsedSql.Token(ParsedSql.TokenType.QUOTED_IDENTIFIER, "\"def\""));
            }

            @Test
            void parameterTokenIsEndedByQuoteCharacter() {
                assertSingleStatementEquals("$1+",
                    new ParsedSql.Token(ParsedSql.TokenType.PARAMETER, "$1"),
                    new ParsedSql.Token(ParsedSql.TokenType.SPECIAL_OR_OPERATOR, "+"));
            }

            @Test
            void parameterIsRecognizedBetweenSpecialCharacters() {
                assertSingleStatementEquals("($1)",
                    new ParsedSql.Token(ParsedSql.TokenType.SPECIAL_OR_OPERATOR, "("),
                    new ParsedSql.Token(ParsedSql.TokenType.PARAMETER, "$1"),
                    new ParsedSql.Token(ParsedSql.TokenType.SPECIAL_OR_OPERATOR, ")")
                );
            }

            @Test
            void lineCommentIsEndedAtNewline() {
                assertSingleStatementEquals("--abc\ndef",
                    new ParsedSql.Token(ParsedSql.TokenType.COMMENT, "--abc"),
                    new ParsedSql.Token(ParsedSql.TokenType.DEFAULT, "def"));
            }

            @Test
            void multipleOperatorsAreSeparatelyTokenized() {
                assertSingleStatementEquals("**",
                    new ParsedSql.Token(ParsedSql.TokenType.SPECIAL_OR_OPERATOR, "*"),
                    new ParsedSql.Token(ParsedSql.TokenType.SPECIAL_OR_OPERATOR, "*")
                );
            }

        }

        @Nested
        class AssortedRealStatementTests {

            @Test
            void simpleSelectStatementIsTokenized() {
                assertSingleStatementEquals("SELECT * FROM /* A Comment */ table WHERE \"SELECT\" = $1",
                    new ParsedSql.Token(ParsedSql.TokenType.DEFAULT, "SELECT"),
                    new ParsedSql.Token(ParsedSql.TokenType.SPECIAL_OR_OPERATOR, "*"),
                    new ParsedSql.Token(ParsedSql.TokenType.DEFAULT, "FROM"),
                    new ParsedSql.Token(ParsedSql.TokenType.COMMENT, "/* A Comment */"),
                    new ParsedSql.Token(ParsedSql.TokenType.DEFAULT, "table"),
                    new ParsedSql.Token(ParsedSql.TokenType.DEFAULT, "WHERE"),
                    new ParsedSql.Token(ParsedSql.TokenType.QUOTED_IDENTIFIER, "\"SELECT\""),
                    new ParsedSql.Token(ParsedSql.TokenType.SPECIAL_OR_OPERATOR, "="),
                    new ParsedSql.Token(ParsedSql.TokenType.PARAMETER, "$1")
                );
            }

            @Test
            void simpleSelectStatementWithFunctionBodyIsTokenized() {
                assertSingleStatementEquals("CREATE FUNCTION test() BEGIN ATOMIC SELECT 1; SELECT 2; END",
                    new ParsedSql.Token(ParsedSql.TokenType.DEFAULT, "CREATE"),
                    new ParsedSql.Token(ParsedSql.TokenType.DEFAULT, "FUNCTION"),
                    new ParsedSql.Token(ParsedSql.TokenType.DEFAULT, "test"),
                    new ParsedSql.Token(ParsedSql.TokenType.SPECIAL_OR_OPERATOR, "("),
                    new ParsedSql.Token(ParsedSql.TokenType.SPECIAL_OR_OPERATOR, ")"),
                    new ParsedSql.Token(ParsedSql.TokenType.DEFAULT, "BEGIN"),
                    new ParsedSql.Token(ParsedSql.TokenType.DEFAULT, "ATOMIC"),
                    new ParsedSql.Token(ParsedSql.TokenType.DEFAULT, "SELECT"),
                    new ParsedSql.Token(ParsedSql.TokenType.DEFAULT, "1"),
                    new ParsedSql.Token(ParsedSql.TokenType.STATEMENT_END, ";"),
                    new ParsedSql.Token(ParsedSql.TokenType.DEFAULT, "SELECT"),
                    new ParsedSql.Token(ParsedSql.TokenType.DEFAULT, "2"),
                    new ParsedSql.Token(ParsedSql.TokenType.STATEMENT_END, ";"),
                    new ParsedSql.Token(ParsedSql.TokenType.DEFAULT, "END")
                );
            }

        }

        void assertSingleStatementEquals(String sql, ParsedSql.Token... tokens) {
            ParsedSql parsedSql = PostgresqlSqlParser.parse(sql);
            assertEquals(1, parsedSql.getStatements().size(), "Parse returned zero or more than 2 statements");
            ParsedSql.Statement statement = parsedSql.getStatements().get(0);
            assertIterableEquals(Arrays.asList(tokens), statement.getTokens());
        }

    }

    @Nested
    class MultipleStatementTests {

        @Test
        void simpleMultipleStatementIsTokenized() {
            ParsedSql parsedSql = PostgresqlSqlParser.parse("DELETE * FROM X; SELECT 1;");
            List<ParsedSql.Statement> statements = parsedSql.getStatements();
            assertEquals(2, statements.size());
            ParsedSql.Statement statementA = statements.get(0);
            ParsedSql.Statement statementB = statements.get(1);

            assertIterableEquals(
                Arrays.asList(
                    new ParsedSql.Token(ParsedSql.TokenType.DEFAULT, "DELETE"),
                    new ParsedSql.Token(ParsedSql.TokenType.SPECIAL_OR_OPERATOR, "*"),
                    new ParsedSql.Token(ParsedSql.TokenType.DEFAULT, "FROM"),
                    new ParsedSql.Token(ParsedSql.TokenType.DEFAULT, "X"),
                    new ParsedSql.Token(ParsedSql.TokenType.STATEMENT_END, ";")
                ),
                statementA.getTokens()
            );

            assertIterableEquals(
                Arrays.asList(
                    new ParsedSql.Token(ParsedSql.TokenType.DEFAULT, "SELECT"),
                    new ParsedSql.Token(ParsedSql.TokenType.DEFAULT, "1"),
                    new ParsedSql.Token(ParsedSql.TokenType.STATEMENT_END, ";")
                ),
                statementB.getTokens()
            );

        }

    }

}
