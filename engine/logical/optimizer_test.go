package logical

import (
    "github.com/aleph-zero/flutterdb/engine"
    "github.com/aleph-zero/flutterdb/engine/ast"
    "github.com/aleph-zero/flutterdb/engine/parser"
    "github.com/aleph-zero/flutterdb/engine/token"
    "github.com/aleph-zero/flutterdb/service/metastore"
    "github.com/google/go-cmp/cmp"
    "github.com/google/go-cmp/cmp/cmpopts"
    "github.com/stretchr/testify/require"
    "testing"
    "text/scanner"
)

const data = "../../testdata/metastore"

func Test_OptimizeConstantProjectionExpression(t *testing.T) {
    teardown, meta := setupSuite(t, data)
    defer teardown(t)

    tests := []struct {
        stmt     string
        expected ast.ExpressionNode
    }{
        {`SELECT 1`, ast.NewIntegerLiteralNode(1)},
        {`SELECT -1`,
            ast.NewUnaryExpressionNode(
                token.Token{TokenType: token.MINUS, Lexeme: "-", Position: scanner.Position{}},
                ast.NewIntegerLiteralNode(1)),
        },
        {`SELECT 1 + 2`, ast.NewIntegerLiteralNode(3)},
        {`SELECT 1 + 2.5`, ast.NewFloatLiteralNode(3.5)},
        {`SELECT 1 + (2 * 3)`, ast.NewIntegerLiteralNode(7)},
        {`SELECT 1 * "a"`, ast.NewIntegerLiteralNode(0)},
        {`SELECT 1 + "a"`, ast.NewIntegerLiteralNode(1)},
        {`SELECT 1 + "-5"`, ast.NewFloatLiteralNode(-4)},
        {`SELECT 2 * "12.2"`, ast.NewFloatLiteralNode(24.4)},
        {`SELECT 1 * c3 FROM t1`,
            ast.NewBinaryExpressionNode(
                token.Token{TokenType: token.ASTERISK, Lexeme: "*", Position: scanner.Position{}},
                ast.NewIntegerLiteralNode(1),
                ast.NewColumnIdentifierNode("c3"),
            ),
        },
        {`SELECT c3 + (2 * (4 + 5)) FROM t1`,
            ast.NewBinaryExpressionNode(
                token.Token{TokenType: token.PLUS, Lexeme: "+", Position: scanner.Position{}},
                ast.NewColumnIdentifierNode("c3"),
                ast.NewIntegerLiteralNode(18),
            ),
        },
        {`SELECT (2 * (4 + 5)) + c3 FROM t1`,
            ast.NewBinaryExpressionNode(
                token.Token{TokenType: token.PLUS, Lexeme: "+", Position: scanner.Position{}},
                ast.NewIntegerLiteralNode(18),
                ast.NewColumnIdentifierNode("c3"),
            ),
        },
        {`SELECT c3 + (c1 * (4 + 5)) FROM t1`,
            ast.NewBinaryExpressionNode(
                token.Token{TokenType: token.PLUS, Lexeme: "+", Position: scanner.Position{}},
                ast.NewColumnIdentifierNode("c3"),
                ast.NewBinaryExpressionNode(
                    token.Token{TokenType: token.ASTERISK, Lexeme: "*", Position: scanner.Position{}},
                    ast.NewColumnIdentifierNode("c1"),
                    ast.NewIntegerLiteralNode(9),
                ),
            ),
        },
    }

    for _, tt := range tests {
        t.Run(tt.stmt, func(t *testing.T) {
            root, err := parse(tt.stmt, meta)
            if err != nil {
                t.Fatal(err)
            }

            plan, err := NewQueryPlan(root)
            if err != nil {
                t.Fatal(err)
            }

            optimizer := NewConstantExpressionEvaluator()
            plan, err = optimizer.optimize(plan)
            if err != nil {
                t.Fatal(err)
            }

            if diff := cmp.Diff(tt.expected, plan.ProjectNode.projections[0],
                cmpopts.IgnoreFields(token.Token{}, "Position"),
                cmpopts.IgnoreFields(ast.ColumnIdentifierNode{}, "ResolvedColumnSymbol"),
            ); diff != "" {
                t.Errorf("failed to optimize plan (-expected, +received):\n%s", diff)
            }
        })
    }
}

func Test_OptimizeConstantSelectionPredicate(t *testing.T) {
    teardown, meta := setupSuite(t, data)
    defer teardown(t)

    tests := []struct {
        stmt     string
        expected ast.ExpressionNode
    }{
        {`SELECT 1 FROM t1 WHERE 1`, ast.NewIntegerLiteralNode(1)},
        {`SELECT 1 FROM t1 WHERE 1 + 2`, ast.NewIntegerLiteralNode(3)},
        {`SELECT 1 FROM t1 WHERE (1 + 2) / c3`,
            ast.NewBinaryExpressionNode(
                token.Token{TokenType: token.DIVIDE, Lexeme: "/", Position: scanner.Position{}},
                ast.NewIntegerLiteralNode(3),
                ast.NewColumnIdentifierNode("c3"),
            ),
        },
        {`SELECT 1 FROM t1 WHERE (1 + 2) / c3 OR ((4 + 5) / 3) > 6`,
            ast.NewBinaryExpressionNode(
                token.Token{TokenType: token.OR, Lexeme: "OR", Position: scanner.Position{}},
                ast.NewBinaryExpressionNode(
                    token.Token{TokenType: token.DIVIDE, Lexeme: "/", Position: scanner.Position{}},
                    ast.NewIntegerLiteralNode(3),
                    ast.NewColumnIdentifierNode("c3"),
                ),
                ast.NewParenthesizedExpressionNode(
                    ast.NewBinaryExpressionNode(
                        token.Token{TokenType: token.GT, Lexeme: ">", Position: scanner.Position{}},
                        ast.NewIntegerLiteralNode(3),
                        ast.NewIntegerLiteralNode(6),
                    )),
            ),
        },
        {`SELECT 1 FROM t1 WHERE NOT 0`,
            ast.NewLogicalNegationNode(
                token.Token{TokenType: token.NOT, Lexeme: "NOT", Position: scanner.Position{}},
                ast.NewIntegerLiteralNode(0)),
        },
        // TODO Add tests for 'AND', 'OR', 'NOT'
    }

    for _, tt := range tests {
        t.Run(tt.stmt, func(t *testing.T) {
            root, err := parse(tt.stmt, meta)
            if err != nil {
                t.Fatal(err)
            }

            plan, err := NewQueryPlan(root)
            if err != nil {
                t.Fatal(err)
            }

            optimizer := NewConstantExpressionEvaluator()
            plan, err = optimizer.optimize(plan)
            if err != nil {
                t.Fatal(err)
            }

            sn := getSelectNode(plan)
            require.NotNil(t, sn)

            if diff := cmp.Diff(tt.expected, sn.Predicate,
                cmpopts.IgnoreFields(token.Token{}, "Position"),
                cmpopts.IgnoreFields(ast.ColumnIdentifierNode{}, "ResolvedColumnSymbol"),
            ); diff != "" {
                t.Errorf("failed to optimize plan (-expected, +received):\n%s", diff)
            }
        })
    }
}

func parse(statement string, meta metastore.Service) (ast.VisitableNode, error) {
    tokens, err := parser.LexicalScan(statement)
    if err != nil {
        return nil, err
    }
    root, err := parser.New(tokens).Parse()
    if err != nil {
        return nil, err
    }
    _, err = engine.ResolveSymbols(meta, root)
    return root, err
}

func setupSuite(tb testing.TB, testdata string) (func(tb testing.TB), metastore.Service) {
    ms := metastore.NewService(testdata)
    if err := ms.Open(); err != nil {
        tb.Fatal(err)
    }
    return func(tb testing.TB) { /* no-op teardown */ }, ms
}
