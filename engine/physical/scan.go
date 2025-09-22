package physical

import (
    "context"
    "fmt"
    "github.com/aleph-zero/flutterdb/engine"
    "github.com/aleph-zero/flutterdb/engine/types"
    "github.com/aleph-zero/flutterdb/service/index"
    "github.com/aleph-zero/flutterdb/service/metastore"
    "github.com/blugelabs/bluge"
    log "github.com/go-chi/httplog/v2"
)

type ScanOperator struct {
    table     *metastore.TableMetadata
    request   bluge.SearchRequest
    indexSvc  index.Service
    sink      chan *engine.Result
    collector *engine.HitCollector // collects hits from the search index
    Stats     ScanOperatorStats
}

func NewScanOperator(indexSvc index.Service, table *metastore.TableMetadata) *ScanOperator {
    return &ScanOperator{
        table:     table,
        indexSvc:  indexSvc,
        request:   bluge.NewAllMatches(bluge.NewMatchAllQuery()),
        collector: engine.NewHitCollector(),
        sink:      make(chan *engine.Result),
    }
}

type ScanOperatorStats struct {
    Records uint64
    Bytes   uint64
}

func (operator *ScanOperator) Sink() <-chan *engine.Result {
    return operator.sink
}

func (operator *ScanOperator) Accept(ctx context.Context, visitor OperatorNodeVisitor) error {
    return visitor.VisitScanOperator(ctx, operator)
}

func (operator *ScanOperator) Open(ctx context.Context) error {
    go func() {
        defer close(operator.sink)
        for result := range operator.collector.Source() {
            operator.Stats.Records++
            operator.Stats.Bytes += uint64(result.Bytes)
            if result.Error != nil {
                // TODO XXX HANDLE ERROR
                log.LogEntry(ctx).Error("Scan error", "queryId", engine.QueryIdFromContext(ctx), "error", result.Error)
            }
            operator.sink <- result
        }
        log.LogEntry(ctx).Info("Scan finished", "records", operator.Stats.Records, "queryId", operator.Stats.Records)
    }()

    return operator.indexSvc.Search(
        ctx,
        operator.table.TableName,
        operator.request,
        operator.collector,
        operator.processor)
}

func (operator *ScanOperator) processor(field string, value []byte) bool {
    if field == "_id" {
        return true
    }
    cmd, ok := operator.table.Columns[field]
    if !ok {
        panic(fmt.Sprintf("unknown field %s", field))
    }

    switch cmd.ColumnType {
    case types.TEXT, types.KEYWORD:
        operator.collector.AddValue(field, engine.NewStringValue(string(value)))
    case types.FLOAT:
        v, err := bluge.DecodeNumericFloat64(value)
        if err != nil {
            operator.collector.Err = fmt.Errorf("error decoding numeric value: %w", err)
        }
        operator.collector.AddValue(field, engine.NewFloatValue(v))
    case types.INTEGER:
        v, err := bluge.DecodeNumericFloat64(value)
        if err != nil {
            operator.collector.Err = fmt.Errorf("error decoding numeric value: %w", err)
        }
        operator.collector.AddValue(field, engine.NewIntValue(int64(v)))
    case types.DATETIME:
        v, err := bluge.DecodeDateTime(value)
        if err != nil {
            operator.collector.Err = fmt.Errorf("error decoding datetime: %w", err)
        }
        operator.collector.AddValue(field, engine.NewTimeValue(v))
    case types.GEOPOINT:
        lat, lon, err := bluge.DecodeGeoLonLat(value)
        if err != nil {
            operator.collector.Err = fmt.Errorf("error decoding geopoint: %w", err)
        }
        operator.collector.AddValue(field, engine.NewGeoPointValue(lat, lon))
    }
    return true
}
