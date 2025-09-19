package physical

import (
    "context"
    "github.com/aleph-zero/flutterdb/engine"
)

type LimitOperator struct {
    child  OperatorNode
    limit  uint64
    source <-chan *engine.Result
    sink   chan *engine.Result
    Stats  LimitOperatorStats
}

type LimitOperatorStats struct {
    Processed uint64
}

func NewLimitOperator(child OperatorNode, limit uint64) *LimitOperator {
    return &LimitOperator{
        limit:  limit,
        child:  child,
        source: child.Sink(),
        sink:   make(chan *engine.Result),
    }
}

func (operator *LimitOperator) Sink() <-chan *engine.Result {
    return operator.sink
}

func (operator *LimitOperator) Accept(ctx context.Context, visitor OperatorNodeVisitor) error {
    return visitor.VisitLimitOperator(ctx, operator)
}

func (operator *LimitOperator) Open(ctx context.Context) error {
    go func() {
        defer close(operator.sink)
        for result := range operator.source {
            if operator.Stats.Processed >= operator.limit {
                // TODO SIGNAL UP THE CHAIN TO TERMINATE
                continue
            }
            operator.Stats.Processed++
            operator.sink <- result
        }
    }()
    return nil
}
