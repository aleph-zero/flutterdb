package index

import (
	"context"
	"fmt"
	"github.com/aleph-zero/flutterdb/engine"
	"github.com/aleph-zero/flutterdb/engine/types"
	"github.com/aleph-zero/flutterdb/service/metastore"
	"github.com/aleph-zero/flutterdb/telemetry"
	"github.com/blugelabs/bluge"
	log "github.com/go-chi/httplog/v2"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"time"
)

type Service interface {
	Index(ctx context.Context, table string, documents []*Document) (*DocumentIndexResult, error)
	Search(ctx context.Context, table string, request bluge.SearchRequest, collector *engine.HitCollector, processor func(string, []byte) bool) error
}

type ServiceProvider struct {
	meta metastore.Service
}

func NewService(meta metastore.Service) *ServiceProvider {
	return &ServiceProvider{
		meta: meta,
	}
}

func (s *ServiceProvider) Search(ctx context.Context, table string, request bluge.SearchRequest, collector *engine.HitCollector, processor func(string, []byte) bool) error {
	ctx, span := telemetry.StartSpan(ctx, "index.Search", trace.WithAttributes(attribute.String("queryId", engine.QueryIdFromContext(ctx))))
	defer span.End()
	log.LogEntry(ctx).Info("Executing search", "table", table)

	tbl, err := s.meta.GetTable(table)
	if err != nil {
		return err
	}

	reader, closer, err := newIndexReader(tbl.Directory)
	if err != nil {
		log.LogEntry(ctx).Error("Error creating index reader", "table", table, "error", err)
		return err
	}
	defer closer()

	dmi, err := reader.Search(ctx, request)
	if err != nil {
		log.LogEntry(ctx).Error("Error searching index", "table", table, "error", err)
		return err
	}

	next, err := dmi.Next()
	for err == nil && next != nil {
		err = next.VisitStoredFields(processor)
		if err != nil {
			panic("visit stored fields") // TODO XXX IMPLEMENT ME
		}
		collector.Bytes = next.Size()
		collector.Emit()
		next, err = dmi.Next()
	}
	collector.Close()

	if err != nil {
		log.LogEntry(ctx).Error("Error iterating search results", "table", table, "error", err)
	}

	return nil
}

func (s *ServiceProvider) Index(ctx context.Context, table string, documents []*Document) (*DocumentIndexResult, error) {
	_, span := otel.GetTracerProvider().Tracer("flutterdb").Start(ctx, "index.Index")
	telemetry.SetAttributes(span)
	defer span.End()
	start := time.Now()
	log.LogEntry(ctx).Info("Indexing documents", "table", table)

	tbl, err := s.meta.GetTable(table)
	if err != nil {
		// TODO - XXX TEST WHAT THIS RETURNS
		return nil, err
	}

	// TODO 1. test error paths
	// TODO 2. test batching into chunks of 1000 docs

	writer, closer, err := newIndexWriter(tbl.Directory)
	if err != nil {
		log.LogEntry(ctx).Error("Error creating index writer", "err", err)
		return nil, err
	}
	defer closer()

	var errorCount = 0
	var successCount = 0
	batch := bluge.NewBatch()
	for _, doc := range documents {
		d := bluge.NewDocument(uuid.New().String())
		for col, val := range doc.Fields {
			cmd, ok := tbl.Columns[col]
			if !ok {
				log.LogEntry(ctx).Error("Column not found", "table", table, "column", col)
				errorCount++
				break
			}
			field, err := createField(col, val, cmd)
			if err != nil {
				log.LogEntry(ctx).Error("Error adding field", "table", table, "column", col, "err", err)
				errorCount++
				break
			}
			d.AddField(field)
		}

		batch.Insert(d)
		successCount++
	}

	if err := writer.Batch(batch); err != nil {
		log.LogEntry(ctx).Error("Writing documents failed", "err", err)
		return nil, err
	}

	log.LogEntry(ctx).Info("Finished indexing documents", "table", table, "success", successCount, "errors", errorCount)
	span.AddEvent("indexer.batch.added",
		trace.WithAttributes(attribute.Int("success", successCount)),
		trace.WithAttributes(attribute.Int("error", errorCount)))

	return &DocumentIndexResult{
		Duration:     time.Since(start),
		Errors:       errorCount,
		Success:      successCount,
		IndexerError: nil,
	}, nil
}

func newIndexReader(path string) (*bluge.Reader, func(), error) {
	reader, err := bluge.OpenReader(bluge.DefaultConfig(path))
	if err != nil {
		return nil, nil, err
	}
	return reader, func() { reader.Close() }, nil
}

func newIndexWriter(path string) (*bluge.Writer, func(), error) {
	writer, err := bluge.OpenWriter(bluge.DefaultConfig(path))
	if err != nil {
		return nil, nil, err
	}
	return writer, func() { writer.Close() }, nil
}

const defaultTextIndexingOptions = bluge.Index | bluge.Sortable | bluge.Store
const defaultKeywordIndexingOptions = bluge.Index | bluge.Sortable | bluge.Store
const defaultNumericIndexingOptions = bluge.Index | bluge.Sortable | bluge.Store | bluge.Aggregatable
const defaultDateTimeIndexingOptions = bluge.Index | bluge.Sortable | bluge.Store | bluge.Aggregatable

func createField(name string, value interface{}, cmd metastore.ColumnMetadata) (bluge.Field, error) {
	switch cmd.ColumnType {
	case types.TEXT:
		v, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("type/value mismatch for column '%s'", cmd.ColumnName)
		}
		f := bluge.NewTextField(name, v)
		f.FieldOptions = defaultTextIndexingOptions
		return f, nil
	case types.KEYWORD:
		v, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("type/value mismatch for column '%s'", cmd.ColumnName)
		}
		f := bluge.NewKeywordField(name, v)
		f.FieldOptions = defaultKeywordIndexingOptions
		return f, nil
	case types.FLOAT, types.INTEGER:
		v, ok := value.(float64)
		if !ok {
			return nil, fmt.Errorf("type/value mismatch for column '%s'", cmd.ColumnName)
		}
		f := bluge.NewNumericField(name, v)
		f.FieldOptions = defaultNumericIndexingOptions
		return f, nil
	case types.DATETIME:
		v, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("type/value mismatch for column '%s'", cmd.ColumnName)
		}
		layout, exists := getTimeLayout(cmd.ColumnOptions["format"])
		if !exists {
			return nil, fmt.Errorf("unsupported datetime format '%s' for column '%s'", cmd.ColumnOptions["format"], cmd.ColumnName)
		}
		t, err := time.Parse(layout, v)
		if err != nil {
			return nil, fmt.Errorf("cannot parse datetime value for column '%s': %w", cmd.ColumnName, err)
		}
		f := bluge.NewDateTimeField(name, t)
		f.FieldOptions = defaultDateTimeIndexingOptions
		return f, nil
	case types.GEOPOINT:
		return nil, fmt.Errorf("geopoint types are not yet supported")
	default:
		return nil, fmt.Errorf("unknown column type: %s", cmd.ColumnType)
	}
}

var timeLayouts = map[string]string{
	"DateTime": time.DateTime,
	"DateOnly": time.DateOnly,
	"TimeOnly": time.TimeOnly,
}

func getTimeLayout(name string) (string, bool) {
	layout, exists := timeLayouts[name]
	return layout, exists
}

type Document struct {
	Fields map[string]interface{}
}

type DocumentIndexResult struct {
	Duration     time.Duration `json:"duration"`
	Errors       int           `json:"errors"`
	Success      int           `json:"success"`
	IndexerError error         `json:"omitempty,indexer_error"`
}

/* *** Errors *** */

type ErrorCode int

const (
	IndexWriterError = iota
)

type Error struct {
	ErrorCode ErrorCode
	Message   string
	Err       error
}
