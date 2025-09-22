package token

import "text/scanner"

type Token struct {
    TokenType
    Lexeme string
    scanner.Position
}

type TokenType int

const (
    IDENTIFIER TokenType = iota

    /* data type token types */

    INTEGER
    FLOAT
    STRING
    TEXT
    KEYWORD
    GEOPOINT
    DATETIME

    /* syntactical token types */

    COMMA
    L_PAREN
    R_PAREN
    BANG

    /* sql keyword token types */

    SELECT
    FROM
    WHERE
    CREATE
    TABLE
    LIMIT
    PARTITION
    BY
    ORDER
    LIKE
    SHOW
    TABLES

    /* arithmetic token types */

    ASTERISK
    PLUS
    MINUS
    DIVIDE
    MODULO

    /* comparison token types */

    EQUAL
    NOT_EQUAL
    GT
    GTE
    LT
    LTE

    /* logical token types */

    AND
    OR
    NOT

    /* misc token types */

    EOF
)

func (t TokenType) String() string {
    return [...]string{
        "IDENTIFIER",
        "INTEGER",
        "FLOAT",
        "STRING",
        "TEXT",
        "KEYWORD",
        "GEOPOINT",
        "DATETIME",
        "COMMA",
        "L_PAREN",
        "R_PAREN",
        "BANG",
        "SELECT",
        "FROM",
        "WHERE",
        "CREATE",
        "TABLE",
        "LIMIT",
        "PARTITION",
        "BY",
        "ORDER",
        "LIKE",
        "SHOW",
        "TABLES",
        "ASTERISK",
        "PLUS",
        "MINUS",
        "DIVIDE",
        "MODULO",
        "EQUAL",
        "NOT_EQUAL",
        "GT",
        "GTE",
        "LT",
        "LTE",
        "AND",
        "OR",
        "NOT",
        "EOF"}[t]
}
