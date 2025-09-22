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
    "go.opentelemetry.io/otel/trace"
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
    shutdown, _ := telemetry.New(serviceName, serviceVersion, collectorURL)
    defer shutdown()

    endpoint := fmt.Sprintf("http://%s:%d/index/%s",
        config.ClientConfig.RemoteAddr, config.ClientConfig.RemotePort, config.Index)

    client := http.Client{
        Transport: otelhttp.NewTransport(http.DefaultTransport),
        Timeout:   time.Second * 30,
    }

    if _, err := os.Stat(config.Filename); os.IsNotExist(err) {
        fmt.Printf("Config file '%s' does not exist\n", config.Filename)
        return
    }

    file, err := os.Open(config.Filename)
    if err != nil {
        fmt.Printf("Error opening file '%s': %s\n", config.Filename, err)
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
            fmt.Printf("Error unmarshalling json: %s\n", err)
            continue
        }
        batch = append(batch, record)
        count++

        if count >= batchSize {
            if err := submitBatch(ctx, endpoint, client, batch, batchNum); err != nil {
                fmt.Printf("Error sending batch: %s\n", err)
            }
            batch = nil
            count = 0
            batchNum++
        }
    }

    if len(batch) > 0 {
        if err := submitBatch(ctx, endpoint, client, batch, batchNum); err != nil {
            fmt.Printf("Error sending batch: %s\n", err)
        }
    }

    if err := scanner.Err(); err != nil {
        fmt.Printf("Error scanning file: %s\n", err)
    }
}

func submitBatch(ctx context.Context, endpoint string, client http.Client, batch []map[string]interface{}, batchNum int) error {
    tr := otel.Tracer(serviceName)
    traceCtx, span := tr.Start(ctx, "client.indexer", trace.WithSpanKind(trace.SpanKindClient))
    defer span.End()

    data, err := json.Marshal(batch)
    if err != nil {
        return err
    }

    req, err := http.NewRequestWithContext(traceCtx, http.MethodPost, endpoint, bytes.NewReader(data))
    if err != nil {
        return err
    }
    req.Header.Set("Content-Type", "application/json")

    resp, err := client.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    fmt.Printf("Batch %d sent; status received: %d", batchNum, resp.Status)
    return nil
}
