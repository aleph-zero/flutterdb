package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/aleph-zero/flutterdb/telemetry"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/trace"
	"log/slog"
	"net/http"
	"os"
	"time"
)

const batchSize = 3000

var collectorURL = os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")

type IndexerConfig struct {
	ClientConfig *Config
	Index        string
	Filename     string
}

type IndexerOption func(*IndexerConfig)

func NewIndexerConfig(options ...IndexerOption) *IndexerConfig {
	cfg := &IndexerConfig{}
	for _, option := range options {
		option(cfg)
	}
	return cfg
}

func WithIndex(index string) IndexerOption {
	return func(cfg *IndexerConfig) {
		cfg.Index = index
	}
}

func WithFilename(filename string) IndexerOption {
	return func(cfg *IndexerConfig) {
		cfg.Filename = filename
	}
}

func WithClientConfig(clientConfig *Config) IndexerOption {
	return func(cfg *IndexerConfig) {
		cfg.ClientConfig = clientConfig
	}
}

func BootstrapIndexer(config *IndexerConfig) {
	ctx := context.Background()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	shutdown, _ := telemetry.New(serviceName, serviceVersion,
		otlptracehttp.WithEndpoint(collectorURL), otlptracehttp.WithInsecure())
	defer shutdown()

	endpoint := fmt.Sprintf("http://%s:%d/index/%s",
		config.ClientConfig.RemoteAddr, config.ClientConfig.RemotePort, config.Index)

	client := http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport),
		Timeout:   time.Second * 30,
	}

	if _, err := os.Stat(config.Filename); os.IsNotExist(err) {
		slog.Error("File does not exist", "filename", config.Filename)
		return
	}

	file, err := os.Open(config.Filename)
	if err != nil {
		slog.Error("Error opening file", "filename", config.Filename, "error", err)
		return
	}
	defer file.Close()

	indexFile(ctx, endpoint, client, file)
}

func indexFile(ctx context.Context, endpoint string, client http.Client, file *os.File) {
	scanner := bufio.NewScanner(file)
	var batch []map[string]interface{}
	count := 0
	batchNum := 1

	for scanner.Scan() {
		var record map[string]interface{}
		line := scanner.Text()
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			slog.Error("Error unmarshalling json", "line", line, "error", err)
			continue
		}
		batch = append(batch, record)
		count++

		if count >= batchSize {
			if err := submitBatch(ctx, endpoint, client, batch, batchNum); err != nil {
				slog.Error("Error sending batch", "batch", batchNum, "error", err)
			}
			batch = nil
			count = 0
			batchNum++
		}
	}

	if len(batch) > 0 {
		if err := submitBatch(ctx, endpoint, client, batch, batchNum); err != nil {
			slog.Error("Error sending batch", "batch", batchNum, "error", err)
		}
	}

	if err := scanner.Err(); err != nil {
		slog.Error("Error scanning file", "file", file.Name(), "error", err)
	}
}

func submitBatch(ctx context.Context, endpoint string, client http.Client, batch []map[string]interface{}, batchNum int) error {
	tr := otel.Tracer(serviceName)
	traceCtx, span := tr.Start(ctx, "client.indexer", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	data, err := json.Marshal(batch)
	if err != nil {
		slog.Error("Error marshalling batch", "batch", batchNum, "error", err)
	}

	req, err := http.NewRequestWithContext(traceCtx, http.MethodPost, endpoint, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("HTTP POST failed: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("HTTP POST failed: %w", err)
	}
	defer resp.Body.Close()

	slog.Info("Batch sent", "batch", batchNum, "status", resp.Status)
	return nil
}
