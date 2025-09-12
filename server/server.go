package server

import (
	"context"
	"errors"
	"fmt"
	"github.com/aleph-zero/flutterdb/api"
	"github.com/aleph-zero/flutterdb/service/cluster"
	"github.com/aleph-zero/flutterdb/service/identity"
	"github.com/aleph-zero/flutterdb/service/index"
	"github.com/aleph-zero/flutterdb/service/membership"
	"github.com/aleph-zero/flutterdb/service/metastore"
	"github.com/aleph-zero/flutterdb/service/query"
	"github.com/aleph-zero/flutterdb/telemetry"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/httplog/v2"
	"github.com/go-chi/render"
	"github.com/riandyrn/otelchi"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	serviceName    = "flutterdb"
	serviceVersion = "0.0.1"
)

var collectorURL = os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")

/* *** Server Config *** */

type Config struct {
	Address         string
	Port            uint16
	ClusterConfig   *ClusterConfig
	MetastoreConfig *metastore.Config
}

type Option func(*Config)

func NewConfig(options ...Option) *Config {
	cfg := &Config{}
	for _, option := range options {
		option(cfg)
	}
	return cfg
}

func WithAddress(address string) Option {
	return func(c *Config) {
		c.Address = address
	}
}

func WithPort(port uint16) Option {
	return func(c *Config) {
		c.Port = port
	}
}

func WithClusterConfig(clusterConfig *ClusterConfig) Option {
	return func(c *Config) {
		c.ClusterConfig = clusterConfig
	}
}

func WithMetastoreConfig(metastoreConfig *metastore.Config) Option {
	return func(c *Config) {
		c.MetastoreConfig = metastoreConfig
	}
}

/* *** Cluster Config *** */

type ClusterConfig struct {
	NodeName             string
	MembershipListenAddr string
	MembershipListenPort uint16
	MembershipJoinAddrs  []string
}

type ClusterOption func(*ClusterConfig)

func NewClusterConfig(options ...ClusterOption) *ClusterConfig {
	cfg := &ClusterConfig{}
	for _, option := range options {
		option(cfg)
	}
	return cfg
}

func WithNodeName(nodeName string) ClusterOption {
	return func(c *ClusterConfig) {
		c.NodeName = nodeName
	}
}

func WithMembershipListenAddr(membershipListenAddr string) ClusterOption {
	return func(c *ClusterConfig) {
		c.MembershipListenAddr = membershipListenAddr
	}
}

func WithMembershipListenPort(membershipListenPort uint16) ClusterOption {
	return func(c *ClusterConfig) {
		c.MembershipListenPort = membershipListenPort
	}
}

func WithMembershipJoinAddrs(membershipJoinAddrs []string) ClusterOption {
	return func(c *ClusterConfig) {
		c.MembershipJoinAddrs = membershipJoinAddrs
	}
}

func Bootstrap(config *Config) {
	ctx, shutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdown()

	logger := httplog.NewLogger(serviceName, httplog.Options{
		LogLevel:         slog.LevelInfo,
		MessageFieldName: "msg",
		JSON:             true,
		Concise:          true,
		RequestHeaders:   false,
		ResponseHeaders:  false,
	})

	logger.InfoContext(ctx, "Bootstrapping server...", "config", config)

	/* *** Initialize Opentelemetry *** */
	shutdown, err := telemetry.New(serviceName, serviceVersion,
		otlptracehttp.WithEndpoint(collectorURL), otlptracehttp.WithInsecure())
	if err != nil {
		logger.ErrorContext(ctx, "Error initializing tracer", err)
	}
	defer shutdown()

	srv := http.Server{
		Addr: fmt.Sprintf("%s:%d", config.Address, config.Port),
	}

	/* *** Initialize Router *** */
	router := chi.NewRouter()
	router.Use(middleware.Heartbeat("/heartbeat"))
	router.Use(otelchi.Middleware(serviceName, otelchi.WithChiRoutes(router)))
	router.Use(middleware.RequestID)
	router.Use(render.SetContentType(render.ContentTypeJSON))
	router.Use(httplog.RequestLogger(logger))

	/* *** Initialize services and handlers, and inject them into the api routes *** */
	metaSvc := metastore.NewService(config.MetastoreConfig.Directory)
	if err := metaSvc.Open(); err != nil {
		logger.ErrorContext(ctx, "Error opening metastore", "err", err)
		os.Exit(1)
	}

	indexSvc := index.NewService(metaSvc)

	{
		handler := api.NewIdentityHandler(identity.NewService(config.ClusterConfig.NodeName))
		router.Get("/identity", handler.GetIdentity)
	}
	{
		handler := api.NewClusterInfoHandler(cluster.NewService(config.Address, config.Port))
		router.Get("/clusterinfo", handler.GetClusterInfo)
	}
	{
		svc, err := membership.NewService(membership.NewMember(
			config.ClusterConfig.NodeName,
			config.ClusterConfig.MembershipListenAddr,
			config.ClusterConfig.MembershipListenPort),
			config.ClusterConfig.MembershipJoinAddrs)
		if err != nil {
			logger.ErrorContext(ctx, "Error creating membership service", "err", err)
			os.Exit(1)
		}
		handler := api.NewMembershipHandler(svc)
		router.Get("/membership", handler.GetMembership)
	}
	{
		handler := api.NewQueryHandler(query.NewService(metaSvc, indexSvc))
		router.Get("/sql", handler.Query)
	}
	{
		handler := api.NewMetastoreHandler(metaSvc)
		router.Put("/metastore/table", handler.Create)
	}
	{
		handler := api.NewIndexerHandler(indexSvc)
		r := chi.NewRouter()
		r.Route("/{index}", func(r chi.Router) {
			r.Use(api.IndexContext)
			r.Post("/", handler.Index)
		})
		router.Mount("/index", r)
	}

	srv.Handler = router
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.ErrorContext(ctx, "Error starting server", "err", err)
		}
		logger.InfoContext(ctx, "Server stopped accepting connections")
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	<-sig

	if err := srv.Shutdown(ctx); err != nil {
		logger.ErrorContext(ctx, "Error shutting down server", "err", err)
		os.Exit(1)
	}
	logger.InfoContext(ctx, "Server shutdown complete")
}
