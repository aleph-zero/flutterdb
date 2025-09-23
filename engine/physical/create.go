package physical

import (
    "context"
    "github.com/aleph-zero/flutterdb/engine"
    "github.com/aleph-zero/flutterdb/engine/ast"
    "github.com/aleph-zero/flutterdb/service/metastore"
)

type CreateOperator struct {
    Name      string
    Columns   []*ast.ColumnDefinitionNode
    Partition string
    metaSvc   metastore.Service
    sink      chan *engine.Result
}

func NewCreateOperator(metaSvc metastore.Service, name string, columns []*ast.ColumnDefinitionNode, partition string) *CreateOperator {
    return &CreateOperator{
        Name:      name,
        Columns:   columns,
        Partition: partition,
        metaSvc:   metaSvc,
        sink:      make(chan *engine.Result),
    }
}

func (operator *CreateOperator) Sink() <-chan *engine.Result {
    return operator.sink
}

func (operator *CreateOperator) Accept(ctx context.Context, visitor OperatorNodeVisitor) error {
    return visitor.VisitCreateOperator(ctx, operator)
}

func (operator *CreateOperator) Open(ctx context.Context) error {
    columns := make(map[string]metastore.ColumnMetadata, len(operator.Columns))
    for _, col := range operator.Columns {
        columns[col.Value] = toColumnMetadata(col)
    }
    tmd := metastore.NewTableMetadata(operator.Name, columns, operator.Partition)
    if err := operator.metaSvc.CreateTable(ctx, tmd); err != nil {
        return err
    }
    return operator.metaSvc.Persist()
}

func toColumnMetadata(column *ast.ColumnDefinitionNode) metastore.ColumnMetadata {
    return metastore.ColumnMetadata{
        ColumnName: column.Value,
        ColumnType: column.Type,
    }
}
