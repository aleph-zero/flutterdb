package logical

import (
	"errors"
	"fmt"
	"github.com/aleph-zero/flutterdb/engine/ast"
	"github.com/aleph-zero/flutterdb/engine/token"
	"golang.org/x/exp/constraints"
	"math"
)

func OptimizeQueryPlan(plan *QueryPlan) (*QueryPlan, error) {
	switch plan.ProjectNode.Child().(type) {
	case *TableNode:
		return plan, nil
	default:
		rules := []OptimizationRule{&ConstantExpressionEvaluator{}}
		for _, rule := range rules {
			var err error
			plan, err = rule.optimize(plan)
			if err != nil {
				return nil, err
			}
		}
		return plan, nil
	}
}

type OptimizationRule interface {
	optimize(*QueryPlan) (*QueryPlan, error)
}

/* *** Constant Expression Optimizer *** */

type ConstantExpressionEvaluator struct {
	expression ast.ExpressionNode
}

func (c *ConstantExpressionEvaluator) optimize(plan *QueryPlan) (*QueryPlan, error) {
	for i, projection := range plan.ProjectNode.projections {
		if err := projection.Accept(c); err != nil {
			return nil, fmt.Errorf("optimizing projection expression: %w", err)
		}
		// replace original expression with optimized version
		plan.ProjectNode.projections[i] = c.expression
	}

	sn := getSelectNode(plan)
	if sn == nil || sn.Predicate == nil {
		return plan, nil
	}

	if err := sn.Predicate.Accept(c); err != nil {
		return nil, fmt.Errorf("optimizing select Predicate: %w", err)
	}

	// replace Predicate with optimized version
	sn.Predicate = c.expression
	return plan, nil
}

func (c *ConstantExpressionEvaluator) VisitBinaryExpressionNode(node *ast.BinaryExpressionNode) error {
	if err := node.Left.Accept(c); err != nil {
		return err
	}
	left := c.expression

	if err := node.Right.Accept(c); err != nil {
		return err
	}
	right := c.expression

	if !ast.IsLiteralNode(left) || !ast.IsLiteralNode(right) {
		c.expression = ast.NewBinaryExpressionNode(node.Op, left, right)
		return nil
	}

	switch node.Op.TokenType {
	case token.PLUS, token.MINUS, token.ASTERISK, token.DIVIDE, token.MODULO:
		expression, err := arithmetic(left.(ast.NumericNode), right.(ast.NumericNode), node.Op.TokenType)
		if err != nil {
			return err
		}
		c.expression = expression
	case token.AND, token.OR, token.NOT:
		panic("and, or, not unimplemented") // TODO XXX DOES NOT WORK WITH 'AND', 'OR', 'NOT'
	default:
		c.expression = node
	}

	return nil
}

func arithmetic(left, right ast.NumericNode, op token.TokenType) (ast.ExpressionNode, error) {
	if left.CanInt() && right.CanInt() {
		l := left.ToInt64()
		r := right.ToInt64()
		if op == token.MODULO {
			return ast.NewIntegerLiteralNode(l % r), nil
		}
		return ast.NewIntegerLiteralNode(apply(l, r, op)), nil
	}

	l := left.ToFloat64()
	r := right.ToFloat64()
	if (op == token.DIVIDE || op == token.MODULO) && r == 0 {
		return nil, errors.New("division by zero")
	}
	if op == token.MODULO {
		return ast.NewFloatLiteralNode(math.Mod(l, r)), nil
	}
	return ast.NewFloatLiteralNode(apply(l, r, op)), nil
}

type operable interface {
	constraints.Integer | constraints.Float
}

func apply[T operable](left, right T, op token.TokenType) T {
	switch op {
	case token.PLUS:
		return left + right
	case token.MINUS:
		return left - right
	case token.DIVIDE:
		return left / right
	case token.ASTERISK:
		return left * right
	default:
		panic("unhandled default case")
	}
	return 0
}

func (c *ConstantExpressionEvaluator) VisitParenthesizedExpression(node *ast.ParenthesizedExpressionNode) error {
	return node.Node.Accept(c)
}

func (c *ConstantExpressionEvaluator) VisitUnaryExpressionNode(node *ast.UnaryExpressionNode) error {
	c.expression = node
	return nil
}

func (c *ConstantExpressionEvaluator) VisitStringLiteralNode(node *ast.StringLiteralNode) error {
	c.expression = node
	return nil
}

func (c *ConstantExpressionEvaluator) VisitIntegerLiteralNode(node *ast.IntegerLiteralNode) error {
	c.expression = node
	return nil
}

func (c *ConstantExpressionEvaluator) VisitFloatLiteralNode(node *ast.FloatLiteralNode) error {
	c.expression = node
	return nil
}

func (c *ConstantExpressionEvaluator) VisitColumnIdentifierNode(node *ast.ColumnIdentifierNode) error {
	c.expression = node
	return nil
}

func (c *ConstantExpressionEvaluator) VisitLimitNode(node *ast.LimitNode) error {
	return fmt.Errorf("cannot optimize node type %T", node)
}

func (c *ConstantExpressionEvaluator) VisitAsteriskLiteralNode(node *ast.AsteriskLiteralNode) error {
	return fmt.Errorf("cannot optimize node type %T", node)
}

func (c *ConstantExpressionEvaluator) VisitLogicalNegationNode(node *ast.LogicalNegationNode) error {
	return fmt.Errorf("cannot optimize node type %T", node)
}

func (c *ConstantExpressionEvaluator) VisitSelectStatementNode(node *ast.SelectStatementNode) error {
	return fmt.Errorf("cannot optimize node type %T", node)
}

func (c *ConstantExpressionEvaluator) VisitPredicateNode(node *ast.PredicateNode) error {
	return fmt.Errorf("cannot optimize node type %T", node)
}

func (c *ConstantExpressionEvaluator) VisitCreateTableStatementNode(node *ast.CreateTableStatementNode) error {
	return fmt.Errorf("cannot optimize node type %T", node)
}

func (c *ConstantExpressionEvaluator) VisitColumnDefinitionNode(node *ast.ColumnDefinitionNode) error {
	return fmt.Errorf("cannot optimize node type %T", node)
}

func (c *ConstantExpressionEvaluator) VisitTableIdentifierNode(node *ast.TableIdentifierNode) error {
	return fmt.Errorf("cannot optimize node type %T", node)
}
