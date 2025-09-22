package parser

import (
    "fmt"
    "github.com/aleph-zero/flutterdb/engine/token"
    "regexp"
    "strings"
    "text/scanner"
)

type TokenPattern struct {
    regex *regexp.Regexp
    token.TokenType
}

var patterns = []TokenPattern{
    {regex: regexp.MustCompile(`(?i)^SELECT$`), TokenType: token.SELECT},
    {regex: regexp.MustCompile(`(?i)^FROM$`), TokenType: token.FROM},
    {regex: regexp.MustCompile(`(?i)^WHERE$`), TokenType: token.WHERE},
    {regex: regexp.MustCompile(`(?i)^CREATE$`), TokenType: token.CREATE},
    {regex: regexp.MustCompile(`(?i)^TABLE$`), TokenType: token.TABLE},
    {regex: regexp.MustCompile(`(?i)^AND$`), TokenType: token.AND},
    {regex: regexp.MustCompile(`(?i)^OR$`), TokenType: token.OR},
    {regex: regexp.MustCompile(`(?i)^NOT$`), TokenType: token.NOT},
    {regex: regexp.MustCompile(`(?i)^LIMIT$`), TokenType: token.LIMIT},
    {regex: regexp.MustCompile(`(?i)^LIKE$`), TokenType: token.LIKE},
    {regex: regexp.MustCompile(`(?i)^SHOW$`), TokenType: token.SHOW},
    {regex: regexp.MustCompile(`(?i)^TABLES$`), TokenType: token.TABLES},
    {regex: regexp.MustCompile(`(?i)^PARTITION$`), TokenType: token.PARTITION},
    {regex: regexp.MustCompile(`(?i)^ORDER$`), TokenType: token.ORDER},
    {regex: regexp.MustCompile(`(?i)^BY$`), TokenType: token.BY},
    {regex: regexp.MustCompile(`(?i)^TEXT$`), TokenType: token.TEXT},
    {regex: regexp.MustCompile(`(?i)^KEYWORD$`), TokenType: token.KEYWORD},
    {regex: regexp.MustCompile(`(?i)^INTEGER$`), TokenType: token.INTEGER},
    {regex: regexp.MustCompile(`(?i)^FLOAT$`), TokenType: token.FLOAT},
    {regex: regexp.MustCompile(`(?i)^DATETIME$`), TokenType: token.DATETIME},
    {regex: regexp.MustCompile(`(?i)^GEOPOINT$`), TokenType: token.GEOPOINT},
    {regex: regexp.MustCompile(`"([^"\\]*(?:\\.[^"\\]*)*)"|'([^'\\]*(?:\\.[^'\\]*)*)'`), TokenType: token.STRING},
    {regex: regexp.MustCompile(`[_a-zA-Z][_a-zA-Z0-9]*`), TokenType: token.IDENTIFIER},
    {regex: regexp.MustCompile(`[0-9]+\.[0-9]+`), TokenType: token.FLOAT},
    {regex: regexp.MustCompile(`\d+`), TokenType: token.INTEGER},
    {regex: regexp.MustCompile(`,`), TokenType: token.COMMA},
    {regex: regexp.MustCompile(`\+`), TokenType: token.PLUS},
    {regex: regexp.MustCompile(`-`), TokenType: token.MINUS},
    {regex: regexp.MustCompile(`/`), TokenType: token.DIVIDE},
    {regex: regexp.MustCompile(`\*`), TokenType: token.ASTERISK},
    {regex: regexp.MustCompile(`%`), TokenType: token.MODULO},
    {regex: regexp.MustCompile(`\(`), TokenType: token.L_PAREN},
    {regex: regexp.MustCompile(`\)`), TokenType: token.R_PAREN},
    {regex: regexp.MustCompile(`!=`), TokenType: token.NOT_EQUAL},
    {regex: regexp.MustCompile(`>=`), TokenType: token.GTE},
    {regex: regexp.MustCompile(`>`), TokenType: token.GT},
    {regex: regexp.MustCompile(`<=`), TokenType: token.LTE},
    {regex: regexp.MustCompile(`<`), TokenType: token.LT},
    {regex: regexp.MustCompile(`<=`), TokenType: token.LTE},
    {regex: regexp.MustCompile(`=`), TokenType: token.EQUAL},
    {regex: regexp.MustCompile(`!`), TokenType: token.BANG},
}

func LexicalScan(src string) ([]token.Token, error) {
    tokens := make([]token.Token, 0, 10)
    var s scanner.Scanner
    s.Init(strings.NewReader(src))

    for tok := s.Scan(); tok != scanner.EOF; tok = s.Scan() {
        matched := false
        text := s.TokenText()
        position := s.Position

        switch {
        case text == "!":
            if s.Peek() == '=' {
                s.Scan()
                text += s.TokenText()
            }
        case text == ">":
            if s.Peek() == '=' {
                s.Scan()
                text += s.TokenText()
            }
        case text == "<":
            if s.Peek() == '=' {
                s.Scan()
                text += s.TokenText()
            }
        }

        for _, pattern := range patterns {
            str := pattern.regex.FindString(text)
            if str != "" {
                matched = true
                if pattern.TokenType == token.STRING {
                    text = strings.Trim(text, "\"")
                }
                tokens = append(tokens, token.Token{
                    TokenType: pattern.TokenType,
                    Lexeme:    text,
                    Position:  position,
                })
                break
            }
        }

        if !matched {
            return nil, fmt.Errorf("unrecognized lexical pattern: %s at position: %s", s.TokenText(), s.Position)
        }
    }

    tokens = append(tokens, token.Token{TokenType: token.EOF})
    return tokens, nil
}
