package physical

import (
    "context"
    "github.com/aleph-zero/flutterdb/engine"
)

type ProjectOperator struct {
    child   OperatorNode
    columns []string
    source  <-chan *engine.Result
    sink    chan *engine.Result
}

func NewProjectOperator(child OperatorNode, columns []string) *ProjectOperator {
    return &ProjectOperator{
        child:   child,
        columns: columns,
        source:  child.Sink(),
        sink:    make(chan *engine.Result),
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
            for k, _ := range result.Record.Values {
                if !exists(k, operator.columns) {
                    delete(result.Record.Values, k)
                }
            }
            operator.sink <- result
        }
    }()
    return nil
}

func exists(key string, values []string) bool {
    for _, value := range values {
        if value == key {
            return true
        }
    }
    return false
}
