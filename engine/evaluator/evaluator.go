package evaluator

import (
	"fmt"
	"github.com/aleph-zero/flutterdb/engine/ast"
	"github.com/aleph-zero/flutterdb/engine/token"
	"math"
)

type Evaluator struct {
	Result float64
}

func (e *Evaluator) VisitSelectStatementNode(node *ast.SelectStatementNode) error {
	for _, expr := range node.Expressions {
		if err := expr.Accept(e); err != nil {
			return err
		}
	}
	return nil
}

func (e *Evaluator) VisitPredicateNode(*ast.PredicateNode) error                       { return nil }
func (e *Evaluator) VisitCreateTableStatementNode(*ast.CreateTableStatementNode) error { return nil }
func (e *Evaluator) VisitColumnDefinitionNode(*ast.ColumnDefinitionNode) error         { return nil }
func (e *Evaluator) VisitTableIdentifierNode(*ast.TableIdentifierNode) error           { return nil }
func (e *Evaluator) VisitColumnIdentifierNode(*ast.ColumnIdentifierNode) error         { return nil }
func (e *Evaluator) VisitLimitNode(*ast.LimitNode) error                               { return nil }

func (e *Evaluator) VisitParenthesizedExpression(node *ast.ParenthesizedExpressionNode) error {
	return node.Node.Accept(e)
}

func (e *Evaluator) VisitLogicalNegationNode(node *ast.LogicalNegationNode) error {
	if err := node.Node.Accept(e); err != nil {
		return err
	}

	switch node.Op.TokenType {
	case token.NOT:
		e.Result = negate(e.Result)
	default:
		panic(fmt.Sprintf("failed to evaluate operator: %s", node.Op.TokenType.String()))
	}
	return nil
}

func (e *Evaluator) VisitUnaryExpressionNode(node *ast.UnaryExpressionNode) error {
	if err := node.Node.Accept(e); err != nil {
		return err
	}

	switch node.Op.TokenType {
	case token.MINUS:
		e.Result = -1 * e.Result
	default:
		return fmt.Errorf("failed to evaluate operator: %s", node.Op.TokenType.String())
	}
	return nil
}

func (e *Evaluator) VisitBinaryExpressionNode(node *ast.BinaryExpressionNode) error {
	if err := node.Left.Accept(e); err != nil {
		return err
	}
	left := e.Result
	if err := node.Right.Accept(e); err != nil {
		return err
	}

	switch node.Op.TokenType {
	case token.PLUS:
		e.Result = left + e.Result
	case token.MINUS:
		e.Result = left - e.Result
	case token.DIVIDE:
		e.Result = left / e.Result
	case token.ASTERISK:
		e.Result = left * e.Result
	case token.MODULO:
		e.Result = math.Mod(left, e.Result)
	case token.GT:
		e.Result = truth(left > e.Result)
	case token.GTE:
		e.Result = truth(left >= e.Result)
	case token.LT:
		e.Result = truth(left < e.Result)
	case token.LTE:
		e.Result = truth(left <= e.Result)
	case token.AND:
		e.Result = and(left, e.Result)
	case token.OR:
		e.Result = or(left, e.Result)
	case token.EQUAL:
		e.Result = equal(left, e.Result)
	case token.NOT_EQUAL:
		e.Result = not_equal(left, e.Result)
	default:
		return fmt.Errorf("failed to evaluate binary operator: %s", node.Op.TokenType.String())
	}
	return nil
}

func (e *Evaluator) VisitStringLiteralNode(node *ast.StringLiteralNode) error {

	// XXX - This is old code. Replace with calls to StringLiteral.NumericValue()

	if len(node.Value) > 0 {
		e.Result = 1
	} else {
		e.Result = 0
	}
	return nil
}

func (e *Evaluator) VisitIntegerLiteralNode(node *ast.IntegerLiteralNode) error {
	e.Result = float64(node.Value)
	return nil
}

func (e *Evaluator) VisitFloatLiteralNode(node *ast.FloatLiteralNode) error {
	e.Result = node.Value
	return nil
}

func (e *Evaluator) VisitAsteriskLiteralNode(node *ast.AsteriskLiteralNode) error { return nil }

func equal(left, right float64) float64 {
	if left == right {
		return 1
	}
	return 0
}

func not_equal(left, right float64) float64 {
	if left != right {
		return 1
	}
	return 0
}

func or(left, right float64) float64 {
	if left != 0 || right != 0 {
		return 1
	}
	return 0
}

func and(left, right float64) float64 {
	if left != 0 && right != 0 {
		return 1
	}
	return 0
}

func negate(f float64) float64 {
	if f == 0 {
		return 1
	}
	return 0
}

func truth(b bool) float64 {
	if b {
		return 1.0
	}
	return 0.0
}
