package io.r2dbc.postgresql.util.sql;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BasicPostgresqlSqlLexerTest {

    @Nested
    class SingleStatementTests {

        @Nested
        class SingleTokenTests {

            @Test
            void singleQuotedStringIsTokenized() {
                assertSingleStatementEqualsCompleteToken("'Test'", TokenType.STRING_CONSTANT);
            }

            @Test
            void dollarQuotedStringIsTokenized() {
                assertSingleStatementEqualsCompleteToken("$$test$$", TokenType.STRING_CONSTANT);
            }

            @Test
            void dollarQuotedTaggedStringIsTokenized() {
                assertSingleStatementEqualsCompleteToken("$a$test$a$", TokenType.STRING_CONSTANT);
            }

            @Test
            void quotedIdentifierIsTokenized() {
                assertSingleStatementEqualsCompleteToken("\"test\"", TokenType.QUOTED_IDENTIFIER);
            }

            @Test
            void lineCommentIsTokenized() {
                assertSingleStatementEqualsCompleteToken("--test", TokenType.COMMENT);
            }

            @Test
            void cStyleCommentIsTokenized() {
                assertSingleStatementEqualsCompleteToken("/*Test*/", TokenType.COMMENT);
            }

            @Test
            void nestedCStyleCommentIsTokenizedAsSingleToken() {
                assertSingleStatementEqualsCompleteToken("/*/*Test*/*/", TokenType.COMMENT);
            }

            @Test
            void windowsMultiLineCStyleCommentIsTokenizedAsSingleToken() {
                assertSingleStatementEqualsCompleteToken("/*Test\r\n Test*/", TokenType.COMMENT);
            }

            @Test
            void unixMultiLineCStyleCommentIsTokenizedAsSingleToken() {
                assertSingleStatementEqualsCompleteToken("/*Test\n Test*/", TokenType.COMMENT);
            }

            @Test
            void digitIsTokenizedAsDefaultToken() {
                assertSingleStatementEqualsCompleteToken("1", TokenType.DEFAULT);
            }

            @Test
            void alphaIsTokenizedAsDefaultToken() {
                assertSingleStatementEqualsCompleteToken("a", TokenType.DEFAULT);
            }

            @Test
            void multipleDefaultTokensAreTokenizedAsSingleDefaultToken() {
                assertSingleStatementEqualsCompleteToken("atest123", TokenType.DEFAULT);
            }

            @Test
            void parameterIsTokenized() {
                assertSingleStatementEqualsCompleteToken("$1", TokenType.PARAMETER);
            }

            @Test
            void statementEndIsTokenized() {
                assertSingleStatementEqualsCompleteToken(";", TokenType.STATEMENT_END);
            }

            void assertSingleStatementEqualsCompleteToken(String sql, TokenType token) {
                assertSingleStatementEquals(sql, new Token(token, sql));
            }

        }

        @Nested
        class SingleTokenExceptionTests {

            @Test
            void unclosedSingleQuotedStringThrowsIllegalArgumentException() {
                assertThrows(IllegalArgumentException.class, () -> BasicPostgresqlSqlLexer.tokenize("'test"));
            }

            @Test
            void unclosedDollarQuotedStringThrowsIllegalArgumentException() {
                assertThrows(IllegalArgumentException.class, () -> BasicPostgresqlSqlLexer.tokenize("$$test"));
            }

            @Test
            void unclosedTaggedDollarQuotedStringThrowsIllegalArgumentException() {
                assertThrows(IllegalArgumentException.class, () -> BasicPostgresqlSqlLexer.tokenize("$abc$test"));
            }

            @Test
            void unclosedQuotedIdentifierThrowsIllegalArgumentException() {
                assertThrows(IllegalArgumentException.class, () -> BasicPostgresqlSqlLexer.tokenize("\"test"));
            }

            @Test
            void unclosedBlockCommentThrowsIllegalArgumentException() {
                assertThrows(IllegalArgumentException.class, () -> BasicPostgresqlSqlLexer.tokenize("/*test"));
            }

            @Test
            void unclosedNestedBlockCommentThrowsIllegalArgumentException() {
                assertThrows(IllegalArgumentException.class, () -> BasicPostgresqlSqlLexer.tokenize("/*/*test*/"));
            }

            @Test
            void invalidParameterCharacterThrowsIllegalArgumentException() {
                assertThrows(IllegalArgumentException.class, () -> BasicPostgresqlSqlLexer.tokenize("$1test"));
            }

            @Test
            void invalidTaggedDollarQuoteThrowsIllegalArgumentException() {
                assertThrows(IllegalArgumentException.class, () -> BasicPostgresqlSqlLexer.tokenize("$a b$test$a b$"));
            }

        }

        @Nested
        class MultipleTokenTests {

            @Test
            void defaultTokenIsEndedBySpecialCharacter() {
                assertSingleStatementEquals("abc[",
                    new Token(TokenType.DEFAULT, "abc"),
                    new Token(TokenType.SPECIAL_OR_OPERATOR, "["));
            }

            @Test
            void defaultTokenIsEndedByOperatorCharacter() {
                assertSingleStatementEquals("abc-",
                    new Token(TokenType.DEFAULT, "abc"),
                    new Token(TokenType.SPECIAL_OR_OPERATOR, "-"));
            }

            @Test
            void defaultTokenIsEndedByStatementEndCharacter() {
                assertSingleStatementEquals("abc;",
                    new Token(TokenType.DEFAULT, "abc"),
                    new Token(TokenType.STATEMENT_END, ";"));
            }

            @Test
            void defaultTokenIsEndedByQuoteCharacter() {
                assertSingleStatementEquals("abc\"def\"",
                    new Token(TokenType.DEFAULT, "abc"),
                    new Token(TokenType.QUOTED_IDENTIFIER, "\"def\""));
            }

            @Test
            void parameterTokenIsEndedByQuoteCharacter() {
                assertSingleStatementEquals("$1+",
                    new Token(TokenType.PARAMETER, "$1"),
                    new Token(TokenType.SPECIAL_OR_OPERATOR, "+"));
            }

            @Test
            void parameterIsRecognizedBetweenSpecialCharacters() {
                assertSingleStatementEquals("($1)",
                    new Token(TokenType.SPECIAL_OR_OPERATOR, "("),
                    new Token(TokenType.PARAMETER, "$1"),
                    new Token(TokenType.SPECIAL_OR_OPERATOR, ")")
                );
            }

            @Test
            void lineCommentIsEndedAtNewline() {
                assertSingleStatementEquals("--abc\ndef",
                    new Token(TokenType.COMMENT, "--abc"),
                    new Token(TokenType.DEFAULT, "def"));
            }

            @Test
            void multipleOperatorsAreSeparatelyTokenized() {
                assertSingleStatementEquals("**",
                    new Token(TokenType.SPECIAL_OR_OPERATOR, "*"),
                    new Token(TokenType.SPECIAL_OR_OPERATOR, "*")
                );
            }

        }

        @Nested
        class AssortedRealStatementTests {

            @Test
            void simpleSelectStatementIsTokenized() {
                assertSingleStatementEquals("SELECT * FROM /* A Comment */ table WHERE \"SELECT\" = $1",
                    new Token(TokenType.DEFAULT, "SELECT"),
                    new Token(TokenType.SPECIAL_OR_OPERATOR, "*"),
                    new Token(TokenType.DEFAULT, "FROM"),
                    new Token(TokenType.COMMENT, "/* A Comment */"),
                    new Token(TokenType.DEFAULT, "table"),
                    new Token(TokenType.DEFAULT, "WHERE"),
                    new Token(TokenType.QUOTED_IDENTIFIER, "\"SELECT\""),
                    new Token(TokenType.SPECIAL_OR_OPERATOR, "="),
                    new Token(TokenType.PARAMETER, "$1")
                );
            }

        }

        void assertSingleStatementEquals(String sql, Token... tokens) {
            TokenizedSql tokenizedSql = BasicPostgresqlSqlLexer.tokenize(sql);
            assertEquals(1, tokenizedSql.getStatements().size(), "Parse returned zero or more than 2 statements");
            TokenizedStatement statement = tokenizedSql.getStatements().get(0);
            assertEquals(new TokenizedStatement(sql, Arrays.asList(tokens)), statement);
        }

    }

    @Nested
    class MultipleStatementTests {

        @Test
        void simpleMultipleStatementIsTokenized() {
            TokenizedSql tokenizedSql = BasicPostgresqlSqlLexer.tokenize("DELETE * FROM X; SELECT 1;");
            List<TokenizedStatement> statements = tokenizedSql.getStatements();
            assertEquals(2, statements.size());
            TokenizedStatement statementA = statements.get(0);
            TokenizedStatement statementB = statements.get(1);

            assertEquals(new TokenizedStatement("DELETE * FROM X;",
                    Arrays.asList(
                        new Token(TokenType.DEFAULT, "DELETE"),
                        new Token(TokenType.SPECIAL_OR_OPERATOR, "*"),
                        new Token(TokenType.DEFAULT, "FROM"),
                        new Token(TokenType.DEFAULT, "X"),
                        new Token(TokenType.STATEMENT_END, ";")
                    )),
                statementA
            );

            assertEquals(new TokenizedStatement("SELECT 1;",
                    Arrays.asList(
                        new Token(TokenType.DEFAULT, "SELECT"),
                        new Token(TokenType.DEFAULT, "1"),
                        new Token(TokenType.STATEMENT_END, ";")
                    )),
                statementB
            );

        }

    }

}