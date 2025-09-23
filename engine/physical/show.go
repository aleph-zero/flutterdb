package physical

import (
    "context"
    "github.com/aleph-zero/flutterdb/engine"
    "github.com/aleph-zero/flutterdb/service/metastore"
)

type ShowTablesOperator struct {
    metaSvc metastore.Service
    sink    chan *engine.Result
}

func NewShowTablesOperator(metaSvc metastore.Service) *ShowTablesOperator {
    return &ShowTablesOperator{
        metaSvc: metaSvc,
        sink:    make(chan *engine.Result),
    }
}

func (operator *ShowTablesOperator) Sink() <-chan *engine.Result {
    return operator.sink
}

func (operator *ShowTablesOperator) Accept(ctx context.Context, visitor OperatorNodeVisitor) error {
    return visitor.VisitShowTablesOperator(ctx, operator)
}

func (operator *ShowTablesOperator) Open(ctx context.Context) error {
    go func() {
        defer close(operator.sink)
        tables := operator.metaSvc.GetTables()
        for _, table := range tables {
            record := engine.NewRecord()
            record.AddValue("table", engine.NewStringValue(table.TableName))
            operator.sink <- &engine.Result{Record: record}
        }
    }()
    return nil
}
