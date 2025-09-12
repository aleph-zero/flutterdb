package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aleph-zero/flutterdb/telemetry"
	"github.com/chzyer/readline"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/trace"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	serviceName       = "flutterdb-cli"
	serviceVersion    = "0.0.1"
	readlineConfigDir = ".config/flutterdb"
)

type Config struct {
	RemoteAddr string
	RemotePort int
}

type Option func(*Config)

func NewConfig(options ...Option) *Config {
	cfg := &Config{}
	for _, option := range options {
		option(cfg)
	}
	return cfg
}

func WithRemoteAddr(addr string) Option {
	return func(cfg *Config) {
		cfg.RemoteAddr = addr
	}
}

func WithRemotePort(port uint16) Option {
	return func(cfg *Config) {
		cfg.RemotePort = int(port)
	}
}

func Bootstrap(config *Config) {
	ctx := context.Background()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	rl, err := setupReadline()
	if err != nil {
		slog.Error("Error setting up readline config", "error", err)
		return
	}
	defer rl.Close()

	shutdown, _ := telemetry.New(serviceName, serviceVersion,
		otlptracehttp.WithEndpoint(collectorURL), otlptracehttp.WithInsecure())
	defer shutdown()

	endpoint := fmt.Sprintf("http://%s:%d/sql", config.RemoteAddr, config.RemotePort)

	client := http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport),
		Timeout:   time.Second * 30,
	}

	for {
		stmt, err := rl.Readline()
		if errors.Is(err, readline.ErrInterrupt) {
			if len(stmt) == 0 {
				break
			} else {
				continue
			}
		} else if err == io.EOF {
			break
		}

		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		submit(ctx, client, endpoint, stmt)
	}
}

func submit(ctx context.Context, client http.Client, endpoint, statement string) error {
	tr := otel.Tracer(serviceName)
	traceCtx, span := tr.Start(ctx, "client.sql", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	req, err := http.NewRequestWithContext(traceCtx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}

	q := req.URL.Query()
	q.Add("q", statement)
	req.URL.RawQuery = q.Encode()

	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	var results []map[string]interface{}
	err = json.NewDecoder(res.Body).Decode(&results)
	if err != nil {
		return err
	}

	fmt.Printf("Results: %+v\n", results)
	return nil
}

func setupReadline() (rl *readline.Instance, err error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}

	dir := filepath.Join(home, readlineConfigDir)
	err = os.MkdirAll(dir, 0750)
	if err != nil {
		return nil, err
	}

	return readline.NewEx(&readline.Config{
		Prompt:            "\033[31mflutterdb> \033[0m ",
		HistoryFile:       filepath.Join(dir, "flutterdb.history"),
		InterruptPrompt:   "^C",
		EOFPrompt:         "exit",
		HistorySearchFold: true,
	})
}
