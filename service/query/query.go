package query

import (
	"context"
	"fmt"
	"github.com/aleph-zero/flutterdb/engine"
	"github.com/aleph-zero/flutterdb/engine/logical"
	"github.com/aleph-zero/flutterdb/engine/parser"
	"github.com/aleph-zero/flutterdb/engine/physical"
	"github.com/aleph-zero/flutterdb/service/index"
	"github.com/aleph-zero/flutterdb/service/metastore"
	"github.com/aleph-zero/flutterdb/telemetry"
	log "github.com/go-chi/httplog/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"strings"
	"time"
)

type Service interface {
	Execute(ctx context.Context, query string) (*QueryResult, error)
}

type ServiceProvider struct {
	metaSvc  metastore.Service
	indexSvc index.Service
}

func NewService(metaSvc metastore.Service, indexSvc index.Service) Service {
	return &ServiceProvider{
		metaSvc:  metaSvc,
		indexSvc: indexSvc}
}

func (sp *ServiceProvider) Execute(ctx context.Context, query string) (*QueryResult, error) {
	start := time.Now()
	queryId := engine.NewQueryId()
	ctx = engine.WithQueryId(ctx, queryId)

	plan, symbols, err := createQueryPlan(ctx, sp.metaSvc, sp.indexSvc, query)
	if err != nil {
		return nil, err
	}

	tables := symbols.GetTableNames()
	ctx, span := telemetry.StartSpan(ctx, "query.Execute", trace.WithAttributes(
		attribute.String("queryId", queryId),
		attribute.String("db.query.text", query),
		attribute.String("db.collection.name", strings.Join(tables, ","))))
	defer span.End()

	results, err := plan.Execute(ctx)
	if err != nil {
		return nil, err
	}

	for _, result := range results {
		fmt.Println("RESULT IN SERVICE LAYER: %+v\n", result)
	}

	return &QueryResult{
		Duration: time.Since(start),
		Records:  nil,
	}, nil
}

type QueryResult struct {
	Duration time.Duration    `json:"duration"`
	Records  []*engine.Record `json:"records"`
}

func createQueryPlan(ctx context.Context, metaSvc metastore.Service, indexSvc index.Service, query string) (*physical.QueryPlan, *metastore.SymbolTable, error) {
	tokens, err := parser.LexicalScan(query)
	if err != nil {
		log.LogEntry(ctx).Error("Error parsing query", "query", query, "queryId", engine.QueryIdFromContext(ctx), "error", err)
		return nil, nil, err
	}

	ast, err := parser.New(tokens).Parse()
	if err != nil {
		log.LogEntry(ctx).Error("Error parsing query", "query", query, "queryId", engine.QueryIdFromContext(ctx), "error", err)
		return nil, nil, err
	}

	symbols, err := engine.ResolveSymbols(metaSvc, ast)
	if err != nil {
		log.LogEntry(ctx).Error("Error resolving symbols", "query", query, "queryId", engine.QueryIdFromContext(ctx), "error", err)
		return nil, nil, err
	}

	plan, err := logical.NewQueryPlan(ast)
	if err != nil {
		log.LogEntry(ctx).Error("Error creating logical plan", "query", query, "queryId", engine.QueryIdFromContext(ctx), "error", err)
		return nil, nil, err
	}

	plan, err = logical.OptimizeQueryPlan(plan)
	if err != nil {
		log.LogEntry(ctx).Error("Error optimizing logical plan", "query", query, "queryId", engine.QueryIdFromContext(ctx), "error", err)
		return nil, nil, err
	}

	phys, err := physical.NewQueryPlan(metaSvc, indexSvc, plan)
	if err != nil {
		log.LogEntry(ctx).Error("Error creating physical plan", "query", query, "queryId", engine.QueryIdFromContext(ctx), "error", err)
		return nil, nil, err
	}
	return phys, symbols, nil
}
