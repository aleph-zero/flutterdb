package telemetry

import (
    "context"
    "go.opentelemetry.io/contrib/instrumentation/runtime"
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/attribute"
    "go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
    "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
    "go.opentelemetry.io/otel/propagation"
    "go.opentelemetry.io/otel/sdk/metric"
    "go.opentelemetry.io/otel/sdk/resource"
    sdktrace "go.opentelemetry.io/otel/sdk/trace"
    semconv "go.opentelemetry.io/otel/semconv/v1.20.0"
    "go.opentelemetry.io/otel/trace"
    "os"
    "time"
)

const systemName = "flutterdb"

type IgnoreExporterErrorsHandler struct{}

func (IgnoreExporterErrorsHandler) Handle(err error) {}

func New(service, version string, collectorURL string) (func(), error) {
    ctx := context.Background()

    res, err := resource.New(
        ctx,
        resource.WithHost(),
        resource.WithContainer(),
        resource.WithAttributes(semconv.ServiceNameKey.String(service), semconv.ServiceVersion(version)))
    if err != nil {
        return nil, err
    }

    te, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpoint(collectorURL), otlptracehttp.WithInsecure())
    if err != nil {
        return nil, err
    }

    tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(te), sdktrace.WithResource(res))
    otel.SetTracerProvider(tp)
    otel.SetTextMapPropagator(propagation.TraceContext{})

    me, err := otlpmetrichttp.New(ctx, otlpmetrichttp.WithEndpoint(collectorURL), otlpmetrichttp.WithInsecure())
    if err != nil {
        return nil, err
    }

    mp := metric.NewMeterProvider(
        metric.WithResource(res),
        metric.WithReader(metric.NewPeriodicReader(
            me,
            metric.WithProducer(runtime.NewProducer()),
            metric.WithInterval(60*time.Second))))

    // The new runtime metrics do not have sufficient data on gc count or pause time, cgo calls, heap objects, etc. So we use the old metrics.
    os.Setenv("OTEL_GO_X_DEPRECATED_RUNTIME_METRICS", "true")
    runtime.Start(runtime.WithMinimumReadMemStatsInterval(60 * time.Second))
    otel.SetMeterProvider(mp)

    // swallow otel errors so they don't spam stdout
    otel.SetErrorHandler(IgnoreExporterErrorsHandler{})

    return func() {
        _ = tp.Shutdown(context.Background())
        _ = mp.Shutdown(ctx)
    }, nil
}

func SetAttributes(span trace.Span, kv ...attribute.KeyValue) {
    for _, attr := range kv {
        span.SetAttributes(attr)
    }
}

func StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
    opts = append(opts, trace.WithAttributes(attribute.String("db.system.name", systemName)))
    return otel.GetTracerProvider().Tracer(systemName).Start(ctx, name, opts...)
}
