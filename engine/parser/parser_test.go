package parser

import (
    "fmt"
    "github.com/aleph-zero/flutterdb/engine/ast"
    "github.com/aleph-zero/flutterdb/engine/evaluator"
    "github.com/aleph-zero/flutterdb/engine/token"
    "github.com/google/go-cmp/cmp"
    "github.com/google/go-cmp/cmp/cmpopts"
    "testing"
    "text/scanner"
)

func TestParser_ParseCreateStatement(t *testing.T) {
    tests := []struct {
        stmt string
    }{
        {`CREATE TABLE t (c1 KEYWORD)`},
        {`CREATE TABLE t (c1 TEXT, c2 KEYWORD, c3 INTEGER, c4 FLOAT, c5 DATETIME, c6 GEOPOINT)`},
        {`CREATE TABLE t (c1 KEYWORD, c2 INTEGER) PARTITION BY c1`},
    }

    for _, tt := range tests {
        t.Run(tt.stmt, func(t *testing.T) {
            fmt.Println("testing: ", tt.stmt)
            root, err := parse(tt.stmt)
            if err != nil {
                t.Error(err)
            }
            fmt.Printf("node: %v+\n", root)
        })
    }
}

func TestParser_ParseShowTablesStatement(t *testing.T) {
    tests := []struct {
        stmt string
    }{
        {`SHOW TABLES`},
    }

    for _, tt := range tests {
        t.Run(tt.stmt, func(t *testing.T) {
            fmt.Println("testing: ", tt.stmt)
            root, err := parse(tt.stmt)
            if err != nil {
                t.Error(err)
            }
            fmt.Printf("node: %v+\n", root)
        })
    }
}

func TestParser_ParseStatements(t *testing.T) {
    tests := []struct {
        stmt     string
        expected *ast.SelectStatementNode
    }{
        {`SELECT 1`,
            ast.NewSelectStatementNode(
                []ast.ExpressionNode{ast.NewIntegerLiteralNode(1)},
            ),
        },
        {`SELECT -1`,
            ast.NewSelectStatementNode(
                []ast.ExpressionNode{
                    ast.NewUnaryExpressionNode(
                        token.Token{TokenType: token.MINUS, Lexeme: "-", Position: scanner.Position{}},
                        ast.NewIntegerLiteralNode(1)),
                },
            ),
        },
        {`SELECT --1`,
            ast.NewSelectStatementNode(
                []ast.ExpressionNode{
                    ast.NewUnaryExpressionNode(
                        token.Token{TokenType: token.MINUS, Lexeme: "-", Position: scanner.Position{}},
                        ast.NewUnaryExpressionNode(
                            token.Token{TokenType: token.MINUS, Lexeme: "-", Position: scanner.Position{}},
                            ast.NewIntegerLiteralNode(1)),
                    ),
                },
            ),
        },
        {`SELECT 1 + 2`,
            ast.NewSelectStatementNode(
                []ast.ExpressionNode{
                    ast.NewBinaryExpressionNode(
                        token.Token{TokenType: token.PLUS, Lexeme: "+", Position: scanner.Position{}},
                        ast.NewIntegerLiteralNode(1),
                        ast.NewIntegerLiteralNode(2),
                    ),
                },
            ),
        },
        {`SELECT 1 + 2.5`,
            ast.NewSelectStatementNode(
                []ast.ExpressionNode{
                    ast.NewBinaryExpressionNode(
                        token.Token{TokenType: token.PLUS, Lexeme: "+", Position: scanner.Position{}},
                        ast.NewIntegerLiteralNode(1),
                        ast.NewFloatLiteralNode(2.5),
                    ),
                },
            ),
        },
        {`SELECT 1 - 2`,
            ast.NewSelectStatementNode(
                []ast.ExpressionNode{
                    ast.NewBinaryExpressionNode(
                        token.Token{TokenType: token.MINUS, Lexeme: "-", Position: scanner.Position{}},
                        ast.NewIntegerLiteralNode(1),
                        ast.NewIntegerLiteralNode(2),
                    ),
                },
            ),
        },
        {`SELECT 1 + 2 * 3`,
            ast.NewSelectStatementNode(
                []ast.ExpressionNode{
                    ast.NewBinaryExpressionNode(
                        token.Token{TokenType: token.PLUS, Lexeme: "+", Position: scanner.Position{}},
                        ast.NewIntegerLiteralNode(1),
                        ast.NewBinaryExpressionNode(
                            token.Token{TokenType: token.ASTERISK, Lexeme: "*", Position: scanner.Position{}},
                            ast.NewIntegerLiteralNode(2),
                            ast.NewIntegerLiteralNode(3),
                        ),
                    ),
                },
            ),
        },
        {`SELECT (1 + 2) * 3`,
            ast.NewSelectStatementNode(
                []ast.ExpressionNode{
                    ast.NewBinaryExpressionNode(
                        token.Token{TokenType: token.ASTERISK, Lexeme: "*", Position: scanner.Position{}},
                        ast.NewParenthesizedExpressionNode(
                            ast.NewBinaryExpressionNode(
                                token.Token{TokenType: token.PLUS, Lexeme: "+", Position: scanner.Position{}},
                                ast.NewIntegerLiteralNode(1),
                                ast.NewIntegerLiteralNode(2),
                            ),
                        ),
                        ast.NewIntegerLiteralNode(3),
                    ),
                },
            ),
        },
        {`SELECT 1 + "abc"`,
            ast.NewSelectStatementNode(
                []ast.ExpressionNode{
                    ast.NewBinaryExpressionNode(
                        token.Token{TokenType: token.PLUS, Lexeme: "+", Position: scanner.Position{}},
                        ast.NewIntegerLiteralNode(1),
                        ast.NewStringLiteralNode("abc"),
                    ),
                },
            ),
        },
        {`SELECT NOT A`,
            ast.NewSelectStatementNode(
                []ast.ExpressionNode{
                    ast.NewLogicalNegationNode(
                        token.Token{TokenType: token.NOT, Lexeme: "NOT", Position: scanner.Position{}},
                        ast.NewColumnIdentifierNode("A"),
                    ),
                },
            ),
        },
        {`SELECT NOT NOT A`,
            ast.NewSelectStatementNode(
                []ast.ExpressionNode{
                    ast.NewLogicalNegationNode(
                        token.Token{TokenType: token.NOT, Lexeme: "NOT", Position: scanner.Position{}},
                        ast.NewLogicalNegationNode(
                            token.Token{TokenType: token.NOT, Lexeme: "NOT", Position: scanner.Position{}},
                            ast.NewColumnIdentifierNode("A"),
                        ),
                    ),
                },
            ),
        },
        {`SELECT NOT A OR B`,
            ast.NewSelectStatementNode(
                []ast.ExpressionNode{
                    ast.NewBinaryExpressionNode(
                        token.Token{TokenType: token.OR, Lexeme: "OR", Position: scanner.Position{}},
                        ast.NewLogicalNegationNode(
                            token.Token{TokenType: token.NOT, Lexeme: "NOT", Position: scanner.Position{}},
                            ast.NewColumnIdentifierNode("A"),
                        ),
                        ast.NewColumnIdentifierNode("B"),
                    ),
                },
            ),
        },
        {`SELECT NOT A AND B`,
            ast.NewSelectStatementNode(
                []ast.ExpressionNode{
                    ast.NewBinaryExpressionNode(
                        token.Token{TokenType: token.AND, Lexeme: "AND", Position: scanner.Position{}},
                        ast.NewLogicalNegationNode(
                            token.Token{TokenType: token.NOT, Lexeme: "NOT", Position: scanner.Position{}},
                            ast.NewColumnIdentifierNode("A"),
                        ),
                        ast.NewColumnIdentifierNode("B"),
                    ),
                },
            ),
        },
        {`SELECT A OR B OR C`,
            ast.NewSelectStatementNode(
                []ast.ExpressionNode{
                    ast.NewBinaryExpressionNode(
                        token.Token{TokenType: token.OR, Lexeme: "OR", Position: scanner.Position{}},
                        ast.NewBinaryExpressionNode(
                            token.Token{TokenType: token.OR, Lexeme: "OR", Position: scanner.Position{}},
                            ast.NewColumnIdentifierNode("A"),
                            ast.NewColumnIdentifierNode("B"),
                        ),
                        ast.NewColumnIdentifierNode("C"),
                    ),
                },
            ),
        },
        {`SELECT A OR B AND C`,
            ast.NewSelectStatementNode(
                []ast.ExpressionNode{
                    ast.NewBinaryExpressionNode(
                        token.Token{TokenType: token.OR, Lexeme: "OR", Position: scanner.Position{}},
                        ast.NewColumnIdentifierNode("A"),
                        ast.NewBinaryExpressionNode(
                            token.Token{TokenType: token.AND, Lexeme: "AND", Position: scanner.Position{}},
                            ast.NewColumnIdentifierNode("B"),
                            ast.NewColumnIdentifierNode("C"),
                        ),
                    ),
                },
            ),
        },
        {`SELECT A AND B OR C`,
            ast.NewSelectStatementNode(
                []ast.ExpressionNode{
                    ast.NewBinaryExpressionNode(
                        token.Token{TokenType: token.OR, Lexeme: "OR", Position: scanner.Position{}},
                        ast.NewBinaryExpressionNode(
                            token.Token{TokenType: token.AND, Lexeme: "AND", Position: scanner.Position{}},
                            ast.NewColumnIdentifierNode("A"),
                            ast.NewColumnIdentifierNode("B"),
                        ),
                        ast.NewColumnIdentifierNode("C"),
                    ),
                },
            ),
        },
    }
    for _, tt := range tests {
        t.Run(tt.stmt, func(t *testing.T) {
            fmt.Println("testing: ", tt.stmt)
            root, err := parse(tt.stmt)
            if err != nil {
                t.Error(err)
            }

            if diff := cmp.Diff(tt.expected, root,
                cmpopts.IgnoreFields(token.Token{}, "Position")); diff != "" {
                t.Errorf("failed to parse (-expected, +received):\n%s", diff)
            }
        })
    }
}

func TestParser_ParseValidExpressions(t *testing.T) {

    tests := []struct {
        stmt     string
        expected float64
    }{
        // literal expressions
        {`SELECT 1`, 1.0},
        {`SELECT 1.2`, 1.2},

        // unary negation
        {`SELECT -5`, -5.0},
        {`SELECT --5`, 5.0},

        // arithmetic operators
        {`SELECT 5 + 6`, 11.0},
        {`SELECT 1 - 3`, -2.0},
        {`SELECT 11 * 33.0`, 363.0},
        {`SELECT 144 / 12`, 12.0},
        {`SELECT 7 % 4`, 3.0},

        // grouping
        {`SELECT (1)`, 1.0},
        {`SELECT 1 + (2 * 3)`, 7.0},
        {`SELECT 1 + (2 * (21 / 3))`, 15.0},
        {`SELECT -(-5)`, 5.0},

        // order of operations
        {`SELECT 28 - 3 * 5 + 10`, 23.0},
        {`SELECT 3 * 16 + 8 - 225 / 3`, -19.0},

        // comparison operators
        {`SELECT 1 > 0`, 1.0},
        {`SELECT 1 >= 1`, 1.0},
        {`SELECT 1 < 0`, 0.0},
        {`SELECT 2 <= 2.0`, 1.0},
        {`SELECT 5.5 > (2.2 + 3.2)`, 1.0},
        {`SELECT 1 = 1`, 1.0},
        {`SELECT 1 != 1`, 0.0},
        {`SELECT 1 = 0`, 0.0},

        // logical operators
        {`SELECT 0`, 0.0},
        {`SELECT NOT 0`, 1.0},
        {`SELECT NOT 5`, 0.0},
        {`SELECT NOT -5`, 0.0},
        {`SELECT NOT 2 > 1`, 0.0},
        {`SELECT NOT NOT 5`, 1.0},
        {`SELECT NOT NOT NOT 5`, 0.0},
        {`SELECT NOT NOT NOT 1`, 0.0},
        {`SELECT 10 > 9`, 1.0},
        {`SELECT 10 > 9 AND 2 > 1`, 1.0},
        {`SELECT 10 > 9 AND 1 > 2`, 0.0},
        {`SELECT 10 > 9 AND NOT 1 > 2`, 1.0},
        {`SELECT 10 > 9 AND NOT NOT 1 > 2`, 0.0},
        {`SELECT 10 > 9 AND NOT (1 > 2)`, 1.0},
        {`SELECT 10 > 9 OR 1 > 2`, 1.0},
        {`SELECT 0 OR NOT 0`, 1.0},
    }

    for _, tt := range tests {
        t.Run(tt.stmt, func(t *testing.T) {
            fmt.Println("testing: ", tt.stmt)
            root, err := parse(tt.stmt)
            if err != nil {
                t.Error(err)
            }

            visitor := evaluator.Evaluator{}
            if err = root.Accept(&visitor); err != nil {
                t.Fatalf("unexpeted evaluation error: %v+\n", err)
            }

            fmt.Printf("visitor result: %f\n", visitor.Result)
            if tt.expected != visitor.Result {
                t.Fatalf("expected: [%f] received: [%f]", tt.expected, visitor.Result)
            }
        })
    }
}

func TestParser_ParseValidExpressionLists(t *testing.T) {

    tests := []struct {
        stmt     string
        expected int
    }{
        {`SELECT 1, 2, a + 4`, 3},
        {`SELECT (2 + 4), 5, "a"`, 3},
    }

    for _, tt := range tests {
        t.Run(tt.stmt, func(t *testing.T) {
            fmt.Println("testing: ", tt.stmt)
            tokens, err := LexicalScan(tt.stmt)
            if err != nil {
                t.Fatalf("lexical error: %v", err)
            }

            p := New(tokens)
            node, err := p.Parse()
            if err != nil {
                t.Fatalf("%s", err)
            }

            fmt.Printf("node: %v+\n", node)
        })
    }
}

func TestParser_ParseValidStatements(t *testing.T) {

    tests := []struct {
        stmt string
    }{
        {`SELECT "a"`},
        {`SELECT a`},
        {`SELECT a + b`},
        {`SELECT a + (b * c)`},
        {`SELECT 1, 2, 5 * 11`},
        {`SELECT a FROM t`},
        {`SELECT a FROM t WHERE a = 5`},
        {`SELECT a FROM t WHERE a = 5 AND b != 6`},
        {`SELECT a FROM t WHERE a = 5 AND b != 6 AND c > 7`},
        {`SELECT a FROM t WHERE a = 5 AND (b != 6 OR 3 > 4)`},
        {`SELECT a FROM t WHERE a = 5 AND NOT b = 6`},
        {`SELECT a FROM t LIMIT 1`},
        {`SELECT a FROM t WHERE a = 5 AND NOT b = 6 LIMIT 1`},
        {`SELECT a FROM t WHERE a LIKE "%apple%"`},
    }

    for _, tt := range tests {
        t.Run(tt.stmt, func(t *testing.T) {
            fmt.Println("testing: ", tt.stmt)
            root, err := parse(tt.stmt)
            if err != nil {
                t.Error(err)
            }
            fmt.Printf("got tree node: %v\n", root)
        })
    }
}

func TestParser_ParseInvalidStatements(t *testing.T) {

    tests := []struct {
        stmt string
    }{
        {`SELECT 1,`},
        {`SELECT 1 aaa`},
        {`SELECT ( -5`},
        {`SELECT /`},
        {`SELECT 1 > `},
        {`SELECT *, a`},
        {`SELECT 5, *`},
        {`SELECT 1 FROM`},
        {`SELECT FROM`},
        {`SELECT a FROM t WHERE`},
        {`SELECT a FROM t LIMIT 5.1`},
        {`SELECT a FROM t LIMIT "a"`},
        {`SELECT a FROM t LIMIT a`},

        {`CREATE TABLE t ( c1 TEXT`},
        {`CREATE TABLE t ( c1 ABC )`},
        {`CREATE TABLE t`},
        {`CREATE TABLE t (c1 TEXT) PARTITION`},
        {`CREATE TABLE t (c1 TEXT) PARTITION BY`},
        {`CREATE TABLE t (c1 TEXT) PARTITION BY 'a'`},
    }

    for _, tt := range tests {
        t.Run(tt.stmt, func(t *testing.T) {
            fmt.Println("testing: ", tt.stmt)
            _, err := parse(tt.stmt)
            if err == nil {
                t.Fatalf("expected parse error")
            }

            fmt.Printf("err: %v\n", err)
        })
    }
}

func parse(statement string) (ast.VisitableNode, error) {
    tokens, err := LexicalScan(statement)
    //printTokens(tokens)
    if err != nil {
        return nil, err
    }
    return New(tokens).Parse()
}

func printTokens(tokens []token.Token) {
    for _, tok := range tokens {
        fmt.Println("--------------")
        fmt.Println("token type:", tok.TokenType)
        fmt.Println("token lexeme:", tok.Lexeme)
        fmt.Printf("token position: %s\n", tok.Position)
    }
}
