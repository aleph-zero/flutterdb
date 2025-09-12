package ast

import (
	"github.com/aleph-zero/flutterdb/engine/token"
	"github.com/aleph-zero/flutterdb/engine/types"
	"github.com/aleph-zero/flutterdb/service/metastore"
	"strconv"
)

type VisitableNode interface {
	Accept(visitor Visitor) error
}

type ExpressionNode interface {
	Expression()
	VisitableNode
}

func IsLiteralNode(n ExpressionNode) bool {
	switch n.(type) {
	case *IntegerLiteralNode, *FloatLiteralNode, *StringLiteralNode:
		return true
	default:
		return false
	}
}

type NumericNode interface {
	ExpressionNode
	CanInt() bool
	CanFloat() bool
	ToInt64() int64
	ToFloat64() float64
}

type SelectStatementNode struct {
	Expressions []ExpressionNode
	Table       *TableIdentifierNode
	Predicate   *PredicateNode
	Limit       *LimitNode
}

func NewSelectStatementNode(expressions []ExpressionNode) *SelectStatementNode {
	return &SelectStatementNode{Expressions: expressions}
}

func (n *SelectStatementNode) Accept(visitor Visitor) error {
	return visitor.VisitSelectStatementNode(n)
}

type TableIdentifierNode struct {
	Value               string
	ResolvedTableSymbol *metastore.TableScopeSymbolTableEntry
}

func NewTableIdentifierNode(value string) *TableIdentifierNode {
	return &TableIdentifierNode{Value: value}
}

func (n *TableIdentifierNode) Accept(visitor Visitor) error {
	if n != nil {
		return visitor.VisitTableIdentifierNode(n)
	}
	return nil
}

type ColumnIdentifierNode struct {
	Value                string
	ResolvedColumnSymbol *metastore.ColumnScopeSymbolTableEntry
}

func NewColumnIdentifierNode(value string) *ColumnIdentifierNode {
	return &ColumnIdentifierNode{Value: value}
}

func (n *ColumnIdentifierNode) Expression() {}

func (n *ColumnIdentifierNode) Accept(visitor Visitor) error {
	if n != nil {
		return visitor.VisitColumnIdentifierNode(n)
	}
	return nil
}

type PredicateNode struct {
	Node ExpressionNode
}

func NewPredicateNode(node ExpressionNode) *PredicateNode {
	return &PredicateNode{Node: node}
}

func (n *PredicateNode) Accept(visitor Visitor) error {
	if n != nil {
		return visitor.VisitPredicateNode(n)
	}
	return nil
}

type CreateTableStatementNode struct {
	Table             string
	ColumnDefinitions []VisitableNode
	Partition         string
}

func NewCreateTableStatementNode(name string, cds []VisitableNode, partition string) *CreateTableStatementNode {
	return &CreateTableStatementNode{
		Table:             name,
		ColumnDefinitions: cds,
		Partition:         partition,
	}
}

func (n *CreateTableStatementNode) Accept(visitor Visitor) error {
	return visitor.VisitCreateTableStatementNode(n)
}

type ColumnDefinitionNode struct {
	Value string
	Type  types.Type
}

func NewColumnDefinitionNode(name string, typ types.Type) *ColumnDefinitionNode {
	return &ColumnDefinitionNode{
		Value: name,
		Type:  typ,
	}
}

func (n *ColumnDefinitionNode) Accept(visitor Visitor) error {
	return visitor.VisitColumnDefinitionNode(n)
}

type ParenthesizedExpressionNode struct {
	Node ExpressionNode
}

func NewParenthesizedExpressionNode(node ExpressionNode) *ParenthesizedExpressionNode {
	return &ParenthesizedExpressionNode{Node: node}
}

func (n *ParenthesizedExpressionNode) Expression() {}

func (n *ParenthesizedExpressionNode) Accept(visitor Visitor) error {
	return visitor.VisitParenthesizedExpression(n)
}

type LogicalNegationNode struct {
	Op   token.Token
	Node ExpressionNode
}

func NewLogicalNegationNode(op token.Token, node ExpressionNode) *LogicalNegationNode {
	return &LogicalNegationNode{
		Op:   op,
		Node: node,
	}
}

func (n *LogicalNegationNode) Expression() {}

func (n *LogicalNegationNode) Accept(visitor Visitor) error {
	return visitor.VisitLogicalNegationNode(n)
}

type UnaryExpressionNode struct {
	Op   token.Token
	Node ExpressionNode
}

func NewUnaryExpressionNode(op token.Token, node ExpressionNode) *UnaryExpressionNode {
	return &UnaryExpressionNode{
		Op:   op,
		Node: node,
	}
}

func (n *UnaryExpressionNode) Expression() {}

func (n *UnaryExpressionNode) Accept(visitor Visitor) error {
	return visitor.VisitUnaryExpressionNode(n)
}

type BinaryExpressionNode struct {
	Op    token.Token
	Left  ExpressionNode
	Right ExpressionNode
}

func NewBinaryExpressionNode(op token.Token, left, right ExpressionNode) *BinaryExpressionNode {
	return &BinaryExpressionNode{
		Op:    op,
		Left:  left,
		Right: right,
	}
}

func (n *BinaryExpressionNode) Expression() {}

func (n *BinaryExpressionNode) Accept(visitor Visitor) error {
	return visitor.VisitBinaryExpressionNode(n)
}

type StringLiteralNode struct {
	Value string
}

func NewStringLiteralNode(value string) *StringLiteralNode {
	return &StringLiteralNode{Value: value}
}

func (n *StringLiteralNode) Accept(visitor Visitor) error {
	return visitor.VisitStringLiteralNode(n)
}

func (n *StringLiteralNode) Expression() {}
func (n *StringLiteralNode) CanInt() bool {
	if n.CanFloat() {
		return false
	}
	return true
}
func (n *StringLiteralNode) CanFloat() bool {
	if _, err := strconv.ParseFloat(n.Value, 64); err == nil {
		return true
	}
	return false
}
func (n *StringLiteralNode) ToInt64() int64 {
	v, err := strconv.ParseInt(n.Value, 10, 64)
	if err != nil {
		return 0
	}
	return v
}
func (n *StringLiteralNode) ToFloat64() float64 {
	v, err := strconv.ParseFloat(n.Value, 64)
	if err != nil {
		return 0
	}
	return v
}

type IntegerLiteralNode struct {
	Value int64
}

func NewIntegerLiteralNode(value int64) *IntegerLiteralNode {
	return &IntegerLiteralNode{Value: value}
}

func (n *IntegerLiteralNode) Accept(visitor Visitor) error {
	return visitor.VisitIntegerLiteralNode(n)
}

func (n *IntegerLiteralNode) Expression()        {}
func (n *IntegerLiteralNode) CanInt() bool       { return true }
func (n *IntegerLiteralNode) CanFloat() bool     { return true }
func (n *IntegerLiteralNode) ToInt64() int64     { return n.Value }
func (n *IntegerLiteralNode) ToFloat64() float64 { return float64(n.Value) }

type FloatLiteralNode struct {
	Value float64
}

func NewFloatLiteralNode(value float64) *FloatLiteralNode {
	return &FloatLiteralNode{Value: value}
}

func (n *FloatLiteralNode) Accept(visitor Visitor) error {
	return visitor.VisitFloatLiteralNode(n)
}

func (n *FloatLiteralNode) Expression()        {}
func (n *FloatLiteralNode) CanInt() bool       { return false } // avoid lossy truncation
func (n *FloatLiteralNode) CanFloat() bool     { return true }
func (n *FloatLiteralNode) ToInt64() int64     { panic("attempt to convert float to int") }
func (n *FloatLiteralNode) ToFloat64() float64 { return n.Value }

type AsteriskLiteralNode struct{}

func NewAsteriskLiteralNode() *AsteriskLiteralNode {
	return &AsteriskLiteralNode{}
}

func (n *AsteriskLiteralNode) Expression() {}

func (n *AsteriskLiteralNode) Accept(visitor Visitor) error {
	return visitor.VisitAsteriskLiteralNode(n)
}

type LimitNode struct {
	Limit IntegerLiteralNode
}

func NewLimitNode(limit IntegerLiteralNode) *LimitNode {
	return &LimitNode{Limit: limit}
}

func (n *LimitNode) Accept(visitor Visitor) error {
	return visitor.VisitLimitNode(n)
}
