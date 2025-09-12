package engine

import (
    "context"
    "fmt"
    "github.com/google/uuid"
    "sort"
    "strings"
    "time"
)

type contextKey string

const queryIdKey contextKey = "queryId"

func NewQueryId() string {
    return uuid.NewString()
}

func WithQueryId(ctx context.Context, queryId string) context.Context {
    return context.WithValue(ctx, queryIdKey, queryId)
}

func QueryIdFromContext(ctx context.Context) string {
    v, ok := ctx.Value(queryIdKey).(string)
    if !ok {
        return ""
    }
    return v
}

type HitCollector struct {
    record *Record
    Bytes  int
    Err    error
    ch     chan *Result
}

func NewHitCollector() *HitCollector {
    return &HitCollector{
        record: NewRecord(),
        ch:     make(chan *Result),
    }
}

func (hc *HitCollector) Source() <-chan *Result {
    return hc.ch
}

func (hc *HitCollector) Emit() {
    hc.ch <- &Result{Record: hc.record, Bytes: hc.Bytes, Error: hc.Err}
    hc.record = NewRecord()
}

func (hc *HitCollector) Close() {
    close(hc.ch)
}

func (hc *HitCollector) AddValue(name string, value Value) {
    hc.record.AddValue(name, value)
}

type Result struct {
    Record *Record
    Bytes  int
    Error  error
}

type Record struct {
    Values map[string]Value
}

func (r *Record) AddValue(name string, value Value) {
    r.Values[name] = value
}

func NewRecord() *Record {
    return &Record{
        Values: make(map[string]Value),
    }
}

func (r *Record) String() string {
    if len(r.Values) == 0 {
        return "{}"
    }

    // Sort keys for consistent output
    keys := make([]string, 0, len(r.Values))
    for k := range r.Values {
        keys = append(keys, k)
    }
    sort.Strings(keys)

    // Build key=value pairs
    var sb strings.Builder
    sb.WriteString("{")
    for i, k := range keys {
        if i > 0 {
            sb.WriteString(", ")
        }
        sb.WriteString(fmt.Sprintf("%s=%s", k, r.Values[k].String()))
    }
    sb.WriteString("}")
    return sb.String()
}

type GeoPointValue struct {
    lat float64
    lon float64
}

type Kind uint8

const (
    Invalid Kind = iota
    String
    Int
    Float
    DateTime
    GeoPoint
)

func (k Kind) String() string {
    switch k {
    case String:
        return "string"
    case Int:
        return "int64"
    case Float:
        return "float64"
    case DateTime:
        return "datetime"
    case GeoPoint:
        return "geopoint"
    default:
        return "invalid"
    }
}

// Value is a compact tagged union for string | int64 | float64 | time.Time.
type Value struct {
    k Kind
    // Only one of these is active, based on k.
    s string
    i int64
    f float64
    t time.Time
    g GeoPointValue
}

// --- Constructors ---

func NewStringValue(v string) Value  { return Value{k: String, s: v} }
func NewIntValue(v int64) Value      { return Value{k: Int, i: v} }
func NewFloatValue(v float64) Value  { return Value{k: Float, f: v} }
func NewTimeValue(v time.Time) Value { return Value{k: DateTime, t: v} }
func NewGeoPointValue(lat, lon float64) Value {
    return Value{k: GeoPoint, g: GeoPointValue{lat: lat, lon: lon}}
}
func InvalidValue() Value { return Value{} }

// --- Introspection ---

func (v Value) Kind() Kind    { return v.k }
func (v Value) IsValid() bool { return v.k != Invalid }

// --- Accessors (type-safe) ---

func (v Value) StringVal() (string, bool)          { return v.s, v.k == String }
func (v Value) IntVal() (int64, bool)              { return v.i, v.k == Int }
func (v Value) FloatVal() (float64, bool)          { return v.f, v.k == Float }
func (v Value) TimeVal() (time.Time, bool)         { return v.t, v.k == DateTime }
func (v Value) GeoPointVal() (GeoPointValue, bool) { return v.g, v.k == GeoPoint }

// Must* helpers (panic on mismatch) — use carefully.

func (v Value) MustString() string {
    if v.k != String {
        panic("not string")
    }
    return v.s
}
func (v Value) MustInt() int64 {
    if v.k != Int {
        panic("not int64")
    }
    return v.i
}
func (v Value) MustFloat() float64 {
    if v.k != Float {
        panic("not float64")
    }
    return v.f
}
func (v Value) MustTime() time.Time {
    if v.k != DateTime {
        panic("not time")
    }
    return v.t
}
func (v Value) MustGeoPoint() GeoPointValue {
    if v.k != GeoPoint {
        panic("not geopoint")
    }
    return v.g
}

// String implements fmt.Stringer (human-readable)
func (v Value) String() string {
    switch v.k {
    case String:
        return v.s
    case Int:
        return fmt.Sprintf("%d", v.i)
    case Float:
        return fmt.Sprintf("%g", v.f)
    case DateTime:
        return v.t.Format(time.RFC3339Nano)
    case GeoPoint:
        return fmt.Sprintf("%f,%f", v.g.lat, v.g.lon)
    default:
        return "<invalid>"
    }
}
