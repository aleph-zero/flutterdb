package physical

import (
    "context"
    "github.com/aleph-zero/flutterdb/engine"
)

type ProjectOperator struct {
    child  OperatorNode
    source <-chan *engine.Result
    sink   chan *engine.Result
}

func NewProjectOperator(child OperatorNode) *ProjectOperator {
    return &ProjectOperator{
        child:  child,
        source: child.Sink(),
        sink:   make(chan *engine.Result),
    }
}

func (operator *ProjectOperator) Sink() <-chan *engine.Result {
    return operator.sink
}

func (operator *ProjectOperator) Accept(ctx context.Context, visitor OperatorNodeVisitor) error {
    return visitor.VisitProjectOperator(ctx, operator)
}

func (operator *ProjectOperator) Open(ctx context.Context) error {
    go func() {
        defer close(operator.sink)
        for result := range operator.source {
            operator.sink <- result
        }
    }()
    return nil
}
