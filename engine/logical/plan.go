package logical

import (
	"fmt"
	"github.com/aleph-zero/flutterdb/engine/ast"
)

type QueryPlan struct {
	ProjectNode ProjectNode
}

type PlanNode interface {
	Accept(PlanNodeVisitor) error
	Child() PlanNode
}

type PlanNodeVisitor interface {
	VisitTableNode(*TableNode) error
	VisitProjectNode(*ProjectNode) error
	VisitSelectNode(*SelectNode) error
	VisitLimitNode(*LimitNode) error
	VisitRelationNode(*RelationNode) error
}

func NewQueryPlan(node ast.VisitableNode) (*QueryPlan, error) {
	switch v := node.(type) {
	case *ast.SelectStatementNode:
		return newSelectStatementPlan(v), nil
	case *ast.CreateTableStatementNode:
		return newCreateTableStatementPlan(v), nil
	default:
		return nil, fmt.Errorf("cannot create query plan for node type: %T", v)
	}
}

func newCreateTableStatementPlan(node *ast.CreateTableStatementNode) *QueryPlan {
	columns := make([]*ast.ColumnDefinitionNode, 0, len(node.ColumnDefinitions))
	for _, cd := range node.ColumnDefinitions {
		columns = append(columns, cd.(*ast.ColumnDefinitionNode))
	}
	project := NewProjectNode(NewTableNode(node.Table, columns, node.Partition), nil)
	return &QueryPlan{ProjectNode: *project}
}

func newSelectStatementPlan(node *ast.SelectStatementNode) *QueryPlan {
	var project *ProjectNode
	if node.Limit != nil {
		project = NewProjectNode(
			NewLimitNode(
				NewSelectNode(NewRelationNode(node.Table), node.Predicate),
				node.Limit.Limit),
			node.Expressions)
	} else {
		project = NewProjectNode(
			NewSelectNode(NewRelationNode(node.Table), node.Predicate), node.Expressions)
	}
	return &QueryPlan{ProjectNode: *project}
}

func getSelectNode(plan *QueryPlan) *SelectNode {
	sn, ok := plan.ProjectNode.child.(*SelectNode)
	if ok {
		return sn
	}
	ln, ok := plan.ProjectNode.child.(*LimitNode)
	if ok {
		sn, ok = ln.child.(*SelectNode)
		if ok {
			return sn
		}
	}
	return nil
}

/* *** Table Node *** */

type TableNode struct {
	Name      string
	Columns   []*ast.ColumnDefinitionNode
	Partition string
}

func NewTableNode(name string, columns []*ast.ColumnDefinitionNode, partition string) *TableNode {
	return &TableNode{
		Name:      name,
		Columns:   columns,
		Partition: partition,
	}
}

func (t *TableNode) Child() PlanNode {
	return nil
}

func (t *TableNode) Accept(visitor PlanNodeVisitor) error {
	return visitor.VisitTableNode(t)
}

/* *** Project Node *** */

type ProjectNode struct {
	projections []ast.ExpressionNode
	child       PlanNode
}

func (p *ProjectNode) Child() PlanNode {
	return p.child
}

func (p *ProjectNode) Accept(visitor PlanNodeVisitor) error {
	return visitor.VisitProjectNode(p)
}

func NewProjectNode(child PlanNode, expressions []ast.ExpressionNode) *ProjectNode {
	projections := make([]ast.ExpressionNode, 0)
	for _, expression := range expressions {
		projections = append(projections, expression)
	}
	return &ProjectNode{
		projections: projections,
		child:       child,
	}
}

/* *** Select Node *** */

type SelectNode struct {
	Predicate ast.ExpressionNode
	child     PlanNode
}

func (s *SelectNode) Child() PlanNode {
	return s.child
}

func (s *SelectNode) Accept(visitor PlanNodeVisitor) error {
	return visitor.VisitSelectNode(s)
}

func NewSelectNode(child PlanNode, predicate *ast.PredicateNode) *SelectNode {
	var p ast.ExpressionNode
	if predicate != nil {
		p = predicate.Node
	} else {
		p = nil
	}
	return &SelectNode{
		Predicate: p,
		child:     child,
	}
}

/* *** Limit Node *** */

type LimitNode struct {
	Limit ast.IntegerLiteralNode
	child PlanNode
}

func (l *LimitNode) Child() PlanNode {
	return l.child
}

func (l *LimitNode) Accept(visitor PlanNodeVisitor) error {
	return visitor.VisitLimitNode(l)
}

func NewLimitNode(child PlanNode, node ast.IntegerLiteralNode) *LimitNode {
	return &LimitNode{
		Limit: node,
		child: child}
}

/* *** Relation Node *** */

type RelationNode struct {
	PushedPredicate ast.ExpressionNode
	Relation        *ast.TableIdentifierNode
}

func (r *RelationNode) Child() PlanNode {
	return nil
}

func (r *RelationNode) Accept(visitor PlanNodeVisitor) error {
	return visitor.VisitRelationNode(r)
}

func NewRelationNode(relation *ast.TableIdentifierNode) *RelationNode {
	return &RelationNode{
		Relation: relation,
	}
}
