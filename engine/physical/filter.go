package physical

import (
	"context"
	"fmt"
	"github.com/aleph-zero/flutterdb/engine"
	"github.com/aleph-zero/flutterdb/engine/ast"
	"github.com/aleph-zero/flutterdb/engine/token"
	"golang.org/x/exp/constraints"
)

type FilterOperator struct {
	child     OperatorNode
	predicate ast.ExpressionNode
	evaluator *PredicateEvaluator
	sink      chan *engine.Result
}

func NewFilterOperator(predicate ast.ExpressionNode, child OperatorNode) *FilterOperator {
	return &FilterOperator{
		child:     child,
		predicate: predicate,
		evaluator: NewPredicateEvaluator(),
		sink:      make(chan *engine.Result),
	}
}

func (operator *FilterOperator) Sink() <-chan *engine.Result {
	return operator.sink
}

func (operator *FilterOperator) Accept(ctx context.Context, visitor OperatorNodeVisitor) error {
	return visitor.VisitFilterOperator(ctx, operator)
}

func (operator *FilterOperator) Open(ctx context.Context) error {
	go func() {
		defer close(operator.sink)
		for result := range operator.child.Sink() {
			if operator.filter(result.Record) {
				// TODO reset evaluator stack here
				operator.sink <- result
			}
		}
	}()
	return nil
}

func (operator *FilterOperator) filter(record *engine.Record) bool {
	operator.evaluator.record = record
	//operator.predicate.Accept(operator.evaluator)
	return true
}

type PredicateEvaluator struct {
	record *engine.Record
	values *engine.Stack[*engine.Value]
}

func NewPredicateEvaluator() *PredicateEvaluator {
	return &PredicateEvaluator{
		values: engine.NewStack[*engine.Value](),
	}
}

func (pe *PredicateEvaluator) comparable() bool {
	return true
}

func (pe *PredicateEvaluator) VisitBinaryExpressionNode(node *ast.BinaryExpressionNode) error {
	if err := node.Left.Accept(pe); err != nil {
		return err
	}
	if err := node.Right.Accept(pe); err != nil {
		return err
	}

	var l, r *engine.Value
	var ok bool
	if r, ok = pe.values.Pop(); !ok {
		panic("empty value stack")
	}
	if l, ok = pe.values.Pop(); !ok {
		panic("empty value stack")
	}

	fmt.Printf("l: %v, r: %v\n", l.String(), r.String())

	switch node.Op.TokenType {
	case token.EQUAL, token.NOT_EQUAL, token.GT, token.GTE, token.LT, token.LTE:
		result := compare(l, r, node.Op.TokenType)
		fmt.Printf("result of comparison: %b on left %s and right %s\n", result, l.String(), r.String())
	default:
		panic("unimplemented operator")
	}

	return nil
}

func compare(left, right *engine.Value, op token.TokenType) bool {
	return true
}

type operable interface {
	constraints.Integer | constraints.Float | string
}

func apply[T operable](left, right T, op token.TokenType) T {
	switch op {
	case token.EQUAL:
	default:
		panic("unhandled default case")
	}

	var zero T
	return zero
}

func (pe *PredicateEvaluator) VisitColumnIdentifierNode(node *ast.ColumnIdentifierNode) error {
	value, ok := pe.record.Values[node.Value]
	if !ok {
		panic(fmt.Sprintf("no record for column %s in result tuple", node.Value))
	}
	pe.values.Push(&value)
	return nil
}

func (pe *PredicateEvaluator) VisitIntegerLiteralNode(node *ast.IntegerLiteralNode) error {
	v := engine.NewIntValue(node.Value)
	pe.values.Push(&v)
	return nil
}

func (pe *PredicateEvaluator) VisitFloatLiteralNode(node *ast.FloatLiteralNode) error {
	v := engine.NewFloatValue(node.Value)
	pe.values.Push(&v)
	return nil
}

func (pe *PredicateEvaluator) VisitParenthesizedExpression(node *ast.ParenthesizedExpressionNode) error {
	//TODO implement me
	panic("implement me")
}

func (pe *PredicateEvaluator) VisitLogicalNegationNode(node *ast.LogicalNegationNode) error {
	//TODO implement me
	panic("implement me")
}

func (pe *PredicateEvaluator) VisitUnaryExpressionNode(node *ast.UnaryExpressionNode) error {
	//TODO implement me
	panic("implement me")
}

func (pe *PredicateEvaluator) VisitStringLiteralNode(node *ast.StringLiteralNode) error {
	//TODO implement me
	panic("implement me")
}

func (pe *PredicateEvaluator) VisitAsteriskLiteralNode(node *ast.AsteriskLiteralNode) error {
	//TODO implement me
	panic("implement me")
}

func (pe *PredicateEvaluator) VisitLimitNode(node *ast.LimitNode) error {
	//TODO implement me
	panic("implement me")
}

func (pe *PredicateEvaluator) VisitPredicateNode(node *ast.PredicateNode) error {
	//TODO implement me
	panic("implement me")
}

func (pe *PredicateEvaluator) VisitSelectStatementNode(node *ast.SelectStatementNode) error {
	return fmt.Errorf("cannot evaluate node type %T", node)
}

func (pe *PredicateEvaluator) VisitCreateTableStatementNode(node *ast.CreateTableStatementNode) error {
	return fmt.Errorf("cannot evaluate node type %T", node)
}

func (pe *PredicateEvaluator) VisitColumnDefinitionNode(node *ast.ColumnDefinitionNode) error {
	return fmt.Errorf("cannot evaluate node type %T", node)
}

func (pe *PredicateEvaluator) VisitTableIdentifierNode(node *ast.TableIdentifierNode) error {
	return fmt.Errorf("cannot evaluate node type %T", node)
}
