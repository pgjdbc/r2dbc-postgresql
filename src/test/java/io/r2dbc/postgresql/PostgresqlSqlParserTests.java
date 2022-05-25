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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link PostgresqlSqlParser}.
 */
class PostgresqlSqlParserTests {

    @Nested
    class SingleStatementTests {

        @Nested
        class SingleTokenTests {

            @Test
            void singleQuotedStringIsTokenized() {
                assertTokenEquals("'Test'", ParsedSql.TokenType.STRING_CONSTANT);
            }

            @Test
            void dollarQuotedStringIsTokenized() {
                assertTokenEquals("$$test$$", ParsedSql.TokenType.STRING_CONSTANT);
            }

            @Test
            void dollarQuotedTaggedStringIsTokenized() {
                assertTokenEquals("$a$test$a$", ParsedSql.TokenType.STRING_CONSTANT);
            }

            @Test
            void quotedIdentifierIsTokenized() {
                assertTokenEquals("\"test\"", ParsedSql.TokenType.QUOTED_IDENTIFIER);
            }

            @Test
            void lineCommentIsTokenized() {
                assertTokenEquals("--test", ParsedSql.TokenType.COMMENT);
            }

            @Test
            void cStyleCommentIsTokenized() {
                assertTokenEquals("/*Test*/", ParsedSql.TokenType.COMMENT);
                assertTokenEquals("/**/", ParsedSql.TokenType.COMMENT);
                assertTokenEquals("/*T*/", ParsedSql.TokenType.COMMENT);
            }

            @Test
            void nestedCStyleCommentIsTokenizedAsSingleToken() {
                assertTokenEquals("/*/*Test*/*/", ParsedSql.TokenType.COMMENT);
            }

            @Test
            void windowsMultiLineCStyleCommentIsTokenizedAsSingleToken() {
                assertTokenEquals("/*Test\r\n Test*/", ParsedSql.TokenType.COMMENT);
            }

            @Test
            void unixMultiLineCStyleCommentIsTokenizedAsSingleToken() {
                assertTokenEquals("/*Test\n Test*/", ParsedSql.TokenType.COMMENT);
            }

            @Test
            void digitIsTokenizedAsDefaultToken() {
                assertTokenEquals("1", ParsedSql.TokenType.DEFAULT);
            }

            @Test
            void alphaIsTokenizedAsDefaultToken() {
                assertTokenEquals("a", ParsedSql.TokenType.DEFAULT);
            }

            @Test
            void multipleDefaultTokensAreTokenizedAsSingleDefaultToken() {
                assertTokenEquals("atest123", ParsedSql.TokenType.DEFAULT);
            }

            @Test
            void parameterIsTokenized() {
                assertTokenEquals("$1", ParsedSql.TokenType.PARAMETER);
            }

            @Test
            void statementEndIsTokenized() {
                assertTokenEquals(";", ParsedSql.TokenType.STATEMENT_END);
            }

            void assertTokenEquals(String sql, ParsedSql.TokenType token) {
                assertSingleStatementEquals(sql, new ParsedSql.Token(token, sql));
            }

        }

        @Nested
        class SingleTokenExceptionTests {

            @Test
            void unclosedSingleQuotedStringThrowsIllegalArgumentException() {
                assertThatIllegalArgumentException().isThrownBy(() -> PostgresqlSqlParser.parse("'test"));
            }

            @Test
            void unclosedDollarQuotedStringThrowsIllegalArgumentException() {
                assertThatIllegalArgumentException().isThrownBy(() -> PostgresqlSqlParser.parse("$$test"));
            }

            @Test
            void unclosedTaggedDollarQuotedStringThrowsIllegalArgumentException() {
                assertThatIllegalArgumentException().isThrownBy(() -> PostgresqlSqlParser.parse("$abc$test"));
            }

            @Test
            void unclosedQuotedIdentifierThrowsIllegalArgumentException() {
                assertThatIllegalArgumentException().isThrownBy(() -> PostgresqlSqlParser.parse("\"test"));
            }

            @Test
            void unclosedBlockCommentThrowsIllegalArgumentException() {
                assertThatIllegalArgumentException().isThrownBy(() -> PostgresqlSqlParser.parse("/*test"));
            }

            @Test
            void unclosedNestedBlockCommentThrowsIllegalArgumentException() {
                assertThatIllegalArgumentException().isThrownBy(() -> PostgresqlSqlParser.parse("/*/*test*/"));
            }

            @Test
            void invalidParameterCharacterThrowsIllegalArgumentException() {
                assertThatIllegalArgumentException().isThrownBy(() -> PostgresqlSqlParser.parse("$1test"));
            }

            @Test
            void invalidTaggedDollarQuoteThrowsIllegalArgumentException() {
                assertThatIllegalArgumentException().isThrownBy(() -> PostgresqlSqlParser.parse("$a b$test$a b$"));
            }

            @Test
            void unclosedTaggedDollarQuoteThrowsIllegalArgumentException() {
                assertThatIllegalArgumentException().isThrownBy(() -> PostgresqlSqlParser.parse("$abc"));
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
            assertThat(parsedSql.getStatements()).hasSize(1);
            ParsedSql.Statement statement = parsedSql.getStatements().get(0);
            assertThat(statement.getTokens()).containsExactly(tokens);
        }

    }

    @Nested
    class MultipleStatementTests {

        @Test
        void simpleMultipleStatementIsTokenized() {
            ParsedSql parsedSql = PostgresqlSqlParser.parse("DELETE * FROM X; SELECT 1;");
            List<ParsedSql.Statement> statements = parsedSql.getStatements();
            assertThat(parsedSql.getStatements()).hasSize(2);
            ParsedSql.Statement statementA = statements.get(0);
            ParsedSql.Statement statementB = statements.get(1);

            assertThat(
                Arrays.asList(
                    new ParsedSql.Token(ParsedSql.TokenType.DEFAULT, "DELETE"),
                    new ParsedSql.Token(ParsedSql.TokenType.SPECIAL_OR_OPERATOR, "*"),
                    new ParsedSql.Token(ParsedSql.TokenType.DEFAULT, "FROM"),
                    new ParsedSql.Token(ParsedSql.TokenType.DEFAULT, "X"),
                    new ParsedSql.Token(ParsedSql.TokenType.STATEMENT_END, ";")
                )
            ).containsExactlyElementsOf(statementA.getTokens());

            assertThat(
                Arrays.asList(
                    new ParsedSql.Token(ParsedSql.TokenType.DEFAULT, "SELECT"),
                    new ParsedSql.Token(ParsedSql.TokenType.DEFAULT, "1"),
                    new ParsedSql.Token(ParsedSql.TokenType.STATEMENT_END, ";")
                )
            ).containsExactlyElementsOf(statementB.getTokens());
        }

    }

}
