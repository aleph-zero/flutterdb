package physical

import (
	"context"
	"github.com/aleph-zero/flutterdb/engine"
	"github.com/aleph-zero/flutterdb/engine/logical"
	"github.com/aleph-zero/flutterdb/service/index"
	"github.com/aleph-zero/flutterdb/service/metastore"
	log "github.com/go-chi/httplog/v2"
)

type QueryPlan struct {
	RootOperator OperatorNode
}

func NewQueryPlan(metaSvc metastore.Service, indexSvc index.Service, plan *logical.QueryPlan) (*QueryPlan, error) {
	visitor := &LogicalPlanVisitor{metaSvc: metaSvc, indexSvc: indexSvc}
	if err := plan.ProjectNode.Accept(visitor); err != nil {
		return nil, err
	}
	return &QueryPlan{RootOperator: visitor.operator}, nil
}

func (plan *QueryPlan) Execute(ctx context.Context) ([]*engine.Result, error) {
	log.LogEntry(ctx).Info("Executing query", "queryId", engine.QueryIdFromContext(ctx))

	results := make([]*engine.Result, 0)

	go func() {
		for result := range plan.RootOperator.Sink() {
			log.LogEntry(ctx).Info("EXECUTE RESULT", "result", result.Record.String())
			results = append(results, result)
		}
	}()

	opener := &OperatorNodeOpener{}
	if err := plan.RootOperator.Accept(ctx, opener); err != nil {
		return nil, err
	}
	return results, nil
}

type OperatorNodeVisitor interface {
	VisitFilterOperator(context.Context, *FilterOperator) error
	VisitLimitOperator(context.Context, *LimitOperator) error
	VisitProjectOperator(context.Context, *ProjectOperator) error
	VisitScanOperator(context.Context, *ScanOperator) error
	VisitCreateOperator(context.Context, *CreateOperator) error
}

type OperatorNode interface {
	Accept(context.Context, OperatorNodeVisitor) error
	Sink() <-chan *engine.Result
	Open(ctx context.Context) error
}

/* *** physical plan opener *** */

type OperatorNodeOpener struct{}

func (op *OperatorNodeOpener) VisitProjectOperator(ctx context.Context, operator *ProjectOperator) error {
	if err := operator.Open(ctx); err != nil {
		return err
	}
	return operator.child.Accept(ctx, op)
}

func (op *OperatorNodeOpener) VisitLimitOperator(ctx context.Context, operator *LimitOperator) error {
	if err := operator.Open(ctx); err != nil {
		return err
	}
	return operator.child.Accept(ctx, op)
}

func (op *OperatorNodeOpener) VisitFilterOperator(ctx context.Context, operator *FilterOperator) error {
	if err := operator.Open(ctx); err != nil {
		return err
	}
	return operator.child.Accept(ctx, op)
}

func (op *OperatorNodeOpener) VisitScanOperator(ctx context.Context, operator *ScanOperator) error {
	return operator.Open(ctx)
}

func (op *OperatorNodeOpener) VisitCreateOperator(ctx context.Context, operator *CreateOperator) error {
	//TODO implement me
	panic("implement me")
}

/* *** logical plan visitor *** */

type LogicalPlanVisitor struct {
	metaSvc  metastore.Service
	indexSvc index.Service
	operator OperatorNode
}

func (lpv *LogicalPlanVisitor) VisitTableNode(node *logical.TableNode) error {
	lpv.operator = NewCreateOperator(lpv.metaSvc, node.Name, node.Columns, node.Partition)
	return nil
}

func (lpv *LogicalPlanVisitor) VisitProjectNode(node *logical.ProjectNode) error {
	if err := node.Child().Accept(lpv); err != nil {
		return err
	}
	lpv.operator = NewProjectOperator(lpv.operator)
	return nil
}

func (lpv *LogicalPlanVisitor) VisitLimitNode(node *logical.LimitNode) error {
	if err := node.Child().Accept(lpv); err != nil {
		return err
	}
	lpv.operator = NewLimitOperator(lpv.operator, uint64(node.Limit.Value))
	return nil
}

func (lpv *LogicalPlanVisitor) VisitSelectNode(node *logical.SelectNode) error {
	if err := node.Child().Accept(lpv); err != nil {
		return err
	}
	lpv.operator = NewFilterOperator(node.Predicate, lpv.operator)
	return nil
}

func (lpv *LogicalPlanVisitor) VisitRelationNode(node *logical.RelationNode) error {
	tmd, err := lpv.metaSvc.GetTable(node.Relation.ResolvedTableSymbol.TableName)
	if err != nil {
		return err
	}
	lpv.operator = NewScanOperator(lpv.indexSvc, tmd)
	return nil
}
