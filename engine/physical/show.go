package physical

import (
    "context"
    "github.com/aleph-zero/flutterdb/engine"
)

type ShowTablesOperator struct{}

func NewShowTablesOperator() *ShowTablesOperator {
    return &ShowTablesOperator{}
}

func (operator *ShowTablesOperator) Sink() <-chan *engine.Result {
    return nil
}

func (operator *ShowTablesOperator) Accept(ctx context.Context, visitor OperatorNodeVisitor) error {
    return nil
}

func (operator *ShowTablesOperator) Open(ctx context.Context) error {
    return nil
}
