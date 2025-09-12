package telemetry

import (
    "context"
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/attribute"
    "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
    "go.opentelemetry.io/otel/propagation"
    "go.opentelemetry.io/otel/sdk/resource"
    sdktrace "go.opentelemetry.io/otel/sdk/trace"
    semconv "go.opentelemetry.io/otel/semconv/v1.20.0"
    "go.opentelemetry.io/otel/trace"
    "log"
)

func New(service, version string, opts ...otlptracehttp.Option) (func(), error) {
    ctx := context.Background()
    exporter, err := otlptracehttp.New(ctx, opts...)
    if err != nil {
        return nil, err
    }

    resources, err := resource.New(
        ctx,
        resource.WithHost(),
        resource.WithContainer(),
        resource.WithAttributes(semconv.ServiceNameKey.String(service), semconv.ServiceVersion(version)))
    if err != nil {
        return nil, err
    }

    tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter), sdktrace.WithResource(resources))
    otel.SetTracerProvider(tp)
    otel.SetTextMapPropagator(propagation.TraceContext{})

    return func() {
        if err := tp.Shutdown(context.Background()); err != nil {
            log.Print("Error shutting down tracer provider", "error", err)
        }
    }, nil
}

func SetAttributes(span trace.Span, kv ...attribute.KeyValue) {
    for _, attr := range kv {
        span.SetAttributes(attr)
    }
}

func StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
    opts = append(opts, trace.WithAttributes(attribute.String("db.system.name", "flutterdb")))
    return otel.GetTracerProvider().Tracer("flutterdb").Start(ctx, name, opts...)
}
