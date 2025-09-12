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
    INTEGER
    FLOAT
    STRING
    COMMA
    L_PAREN
    R_PAREN
    SELECT
    FROM
    WHERE
    CREATE
    TABLE
    ASTERISK
    PLUS
    MINUS
    DIVIDE
    MODULO
    EQUAL
    NOT_EQUAL
    GT
    GTE
    LT
    LTE
    BANG
    AND
    OR
    NOT
    LIMIT
    PARTITION
    BY
    TEXT
    KEYWORD
    GEOPOINT
    DATETIME
    EOF
)

func (t TokenType) String() string {
    return [...]string{
        "IDENTIFIER",
        "INTEGER",
        "FLOAT",
        "STRING",
        "COMMA",
        "L_PAREN",
        "R_PAREN",
        "SELECT",
        "FROM",
        "WHERE",
        "CREATE",
        "TABLE",
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
        "BANG",
        "AND",
        "OR",
        "NOT",
        "LIMIT",
        "PARTITION",
        "BY",
        "TEXT",
        "KEYWORD",
        "GEOPOINT",
        "DATETIME",
        "EOF"}[t]
}
