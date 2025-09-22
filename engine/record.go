package engine

import (
    "context"
    "encoding/json"
    "fmt"
    "github.com/google/uuid"
    "math"
    "sort"
    "strconv"
    "strings"
    "time"
    "unicode/utf8"
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
    Record *Record `json:"record"`
    Bytes  int
    Error  error
}

type Record struct {
    Values map[string]Value `json:"values"`
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
    Boolean
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
    case Boolean:
        return "boolean"
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
    b bool
}

// --- Constructors ---

func NewStringValue(v string) Value  { return Value{k: String, s: v} }
func NewIntValue(v int64) Value      { return Value{k: Int, i: v} }
func NewFloatValue(v float64) Value  { return Value{k: Float, f: v} }
func NewBooleanValue(v bool) Value   { return Value{k: Boolean, b: v} }
func NewTimeValue(v time.Time) Value { return Value{k: DateTime, t: v} }
func NewGeoPointValue(lat, lon float64) Value {
    return Value{k: GeoPoint, g: GeoPointValue{lat: lat, lon: lon}}
}

// --- Introspection ---

func (v Value) Kind() Kind    { return v.k }
func (v Value) IsValid() bool { return v.k != Invalid }

func (v Value) CanInt() bool {
    switch v.k {
    case Float:
        return false
    case Int:
        return true
    case String:
        _, err := strconv.ParseInt(strings.TrimSpace(v.s), 10, 64)
        return err == nil
    case Boolean:
        return true
    default:
        return false
    }
}

func (v Value) CanFloat() bool {
    switch v.k {
    case Float:
        return true
    case Int:
        return true
    case String:
        if _, err := strconv.ParseFloat(v.s, 64); err == nil {
            return true
        }
        return false
    case Boolean:
        return true
    default:
        return false
    }
}

// --- Conversion ---

func (v Value) ToInt() int64 {
    switch v.k {
    case Float:
        panic("attempt to convert float to int")
    case Int:
        return v.i
    case String:
        i, err := strconv.ParseInt(strings.TrimSpace(v.s), 10, 64)
        if err != nil {
            return 0
        }
        return i
    case Boolean:
        if v.b {
            return 1
        }
        return 0
    default:
        return 0
    }
}

func (v Value) ToFloat() float64 {
    switch v.k {
    case Float:
        return v.f
    case Int:
        return float64(v.i)
    case String:
        v, err := strconv.ParseFloat(v.s, 64)
        if err != nil {
            return 0
        }
        return v
    case Boolean:
        if v.b {
            return 1.0
        }
        return 0.0
    default:
        return 0.0
    }
}

func (v Value) ToBoolean() bool {
    switch v.k {
    case Int:
        return v.i != 0
    case Float:
        return v.f != 0
    case Boolean:
        return v.b
    case String:
        s := strings.TrimSpace(strings.ToLower(v.s))
        switch s {
        case "true", "t", "yes", "y", "on":
            return true
        case "false", "f", "no", "n", "off":
            return false
        }
        if i, err := strconv.ParseInt(s, 10, 64); err == nil {
            return i != 0
        }
        if f, err := strconv.ParseFloat(s, 64); err == nil {
            return f != 0 && !math.IsNaN(f)
        }
        return false
    default:
        return false
    }
}

// --- Accessors (type-safe) ---

func (v Value) StringVal() (string, bool)          { return v.s, v.k == String }
func (v Value) IntVal() (int64, bool)              { return v.i, v.k == Int }
func (v Value) FloatVal() (float64, bool)          { return v.f, v.k == Float }
func (v Value) BooleanVal() (bool, bool)           { return v.b, v.k == Boolean }
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
func (v Value) MustBoolean() bool {
    if v.k != Boolean {
        panic("not boolean")
    }
    return v.b
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
        return strconv.Quote(v.s) // avoids ambiguity with numbers/empty
    case Int:
        return fmt.Sprintf("%d", v.i)
    case Float:
        return fmt.Sprintf("%g", v.f)
    case Boolean:
        return fmt.Sprintf("%t", v.b)
    case DateTime:
        return v.t.Format(time.RFC3339Nano)
    case GeoPoint:
        return fmt.Sprintf("%.6f,%.6f", v.g.lat, v.g.lon)
    default:
        return "<invalid>"
    }
}

func (v Value) Equal(u Value) bool {
    if v.k != u.k {
        return false
    }
    switch v.k {
    case String:
        return v.s == u.s
    case Int:
        return v.i == u.i
    case Float:
        return (v.f == u.f) || (math.IsNaN(v.f) && math.IsNaN(u.f))
    case Boolean:
        return v.b == u.b
    case DateTime:
        return v.t.Equal(u.t)
    case GeoPoint:
        return v.g.lat == u.g.lat && v.g.lon == u.g.lon
    default:
        return true // both invalid
    }
}

type wire struct {
    Kind  string      `json:"kind"`
    Value interface{} `json:"value"`
}

func (v Value) MarshalJSON() ([]byte, error) {
    w := wire{Kind: v.k.String()}
    switch v.k {
    case String:
        w.Value = v.s
    case Int:
        w.Value = v.i
    case Float:
        if math.IsNaN(v.f) || math.IsInf(v.f, 0) {
            return nil, fmt.Errorf("non-finite float")
        }
        w.Value = v.f
    case Boolean:
        w.Value = v.b
    case DateTime:
        w.Value = v.t.Format(time.RFC3339Nano)
    case GeoPoint:
        w.Value = map[string]float64{"lat": v.g.lat, "lon": v.g.lon}
    default:
        w.Kind = "invalid"
    }
    return json.Marshal(w)
}

func (k Kind) MarshalText() ([]byte, error) { return []byte(k.String()), nil }

// UnmarshalJSON implements the tagged format: {"kind":"int64","value":123}
func (v *Value) UnmarshalJSON(data []byte) error {
    type wire struct {
        Kind  string          `json:"kind"`
        Value json.RawMessage `json:"value"`
    }
    var w wire
    if err := json.Unmarshal(data, &w); err != nil {
        return err
    }

    k, err := parseKind(w.Kind)
    if err != nil {
        return err
    }

    var out Value
    out.k = k

    switch k {
    case String:
        var s string
        if err := json.Unmarshal(w.Value, &s); err != nil {
            return fmt.Errorf("value(kind=string) must be a JSON string: %w", err)
        }
        out.s = s

    case Int:
        // Accept number or numeric string
        var num json.Number
        if err := json.Unmarshal(w.Value, &num); err == nil {
            i, ierr := strconv.ParseInt(num.String(), 10, 64)
            if ierr != nil {
                return fmt.Errorf("value(kind=int64) not an int64: %v", ierr)
            }
            out.i = i
            break
        }
        var s string
        if err := json.Unmarshal(w.Value, &s); err == nil {
            i, ierr := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
            if ierr != nil {
                return fmt.Errorf("value(kind=int64) not an int64 string: %v", ierr)
            }
            out.i = i
            break
        }
        return fmt.Errorf("value(kind=int64) must be number or base-10 string")

    case Float:
        // Accept number or numeric string
        var f float64
        if err := json.Unmarshal(w.Value, &f); err == nil {
            if math.IsNaN(f) || math.IsInf(f, 0) {
                return fmt.Errorf("value(kind=float64) must be finite")
            }
            out.f = f
            break
        }
        var s string
        if err := json.Unmarshal(w.Value, &s); err == nil {
            fv, ierr := strconv.ParseFloat(strings.TrimSpace(s), 64)
            if ierr != nil || math.IsNaN(fv) || math.IsInf(fv, 0) {
                return fmt.Errorf("value(kind=float64) invalid float string")
            }
            out.f = fv
            break
        }
        return fmt.Errorf("value(kind=float64) must be number or numeric string")

    case Boolean:
        // Accept bool, common strings, or 0/1
        var b bool
        if err := json.Unmarshal(w.Value, &b); err == nil {
            out.b = b
            break
        }
        var s string
        if err := json.Unmarshal(w.Value, &s); err == nil {
            if bv, ok := parseBool(s); ok {
                out.b = bv
                break
            }
            return fmt.Errorf("value(kind=boolean) invalid boolean string %q", s)
        }
        var n json.Number
        if err := json.Unmarshal(w.Value, &n); err == nil {
            i, ierr := strconv.ParseInt(n.String(), 10, 64)
            if ierr == nil {
                out.b = (i != 0)
                break
            }
        }
        return fmt.Errorf("value(kind=boolean) must be bool, boolean string, or 0/1")

    case DateTime:
        // RFC3339 / RFC3339Nano string
        var s string
        if err := json.Unmarshal(w.Value, &s); err != nil {
            return fmt.Errorf("value(kind=datetime) must be a string: %w", err)
        }
        if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
            out.t = t
            break
        }
        if t, err := time.Parse(time.RFC3339, s); err == nil {
            out.t = t
            break
        }
        return fmt.Errorf("value(kind=datetime) must be RFC3339/RFC3339Nano string: %q", s)

    case GeoPoint:
        // Accept object {"lat":..,"lon":..} or array [lat, lon]
        var obj struct {
            Lat float64 `json:"lat"`
            Lon float64 `json:"lon"`
        }
        if err := json.Unmarshal(w.Value, &obj); err == nil {
            out.g = GeoPointValue{lat: obj.Lat, lon: obj.Lon}
            break
        }
        var arr [2]float64
        if err := json.Unmarshal(w.Value, &arr); err == nil {
            out.g = GeoPointValue{lat: arr[0], lon: arr[1]}
            break
        }
        return fmt.Errorf("value(kind=geopoint) must be {\"lat\":..,\"lon\":..} or [lat,lon]")

    case Invalid:
        // Allow {"kind":"invalid"} for explicit invalid values
        out = Value{}
    default:
        return fmt.Errorf("unknown kind %q", w.Kind)
    }

    *v = out
    return nil
}

func parseKind(s string) (Kind, error) {
    switch strings.ToLower(strings.TrimSpace(s)) {
    case "string":
        return String, nil
    case "int", "int64":
        return Int, nil
    case "float", "float64":
        return Float, nil
    case "datetime", "time":
        return DateTime, nil
    case "geopoint", "geo", "point":
        return GeoPoint, nil
    case "boolean", "bool":
        return Boolean, nil
    case "invalid":
        return Invalid, nil
    default:
        return Invalid, fmt.Errorf("unknown kind %q", s)
    }
}

func parseBool(s string) (bool, bool) {
    switch strings.ToLower(strings.TrimSpace(s)) {
    case "true", "t", "yes", "y", "on", "1":
        return true, true
    case "false", "f", "no", "n", "off", "0":
        return false, true
    default:
        return false, false
    }
}

// --- Table options ---
type OverflowMode uint8

const (
    Truncate OverflowMode = iota
    Wrap
)

type RenderOptions struct {
    // Columns: explicit order. If empty, we'll infer from first record (then fill with any extras).
    Columns []string

    // MaxWidth: soft cap per column (content is wrapped/truncated to this).
    // Default: 80 if zero or negative.
    MaxWidth int

    // Overflow: Truncate (default) or Wrap.
    Overflow OverflowMode
}

// RenderASCIITable renders a slice of *Record as an ASCII table with borders.
// - If opts is nil, defaults are used (MaxWidth=80, Overflow=Truncate, infer columns from first record).
func RenderASCIITable(records []*Record, opts *RenderOptions) string {
    if len(records) == 0 {
        return ""
    }
    if opts == nil {
        opts = &RenderOptions{}
    }
    if opts.MaxWidth <= 0 {
        opts.MaxWidth = 80
    }

    columns := make([]string, 0, 16)
    seen := map[string]struct{}{}

    // If explicit columns provided, use them.
    if len(opts.Columns) > 0 {
        for _, c := range opts.Columns {
            if _, ok := seen[c]; !ok {
                seen[c] = struct{}{}
                columns = append(columns, c)
            }
        }
    }

    // Otherwise, infer from FIRST record (then add any extra columns found later, sorted).
    // NOTE: Go maps are unordered; we create a deterministic order by lexicographically sorting
    // the first record's keys. This guarantees stable output, but cannot reflect insertion order.
    if len(columns) == 0 {
        first := records[0]
        firstKeys := make([]string, 0, len(first.Values))
        for k := range first.Values {
            firstKeys = append(firstKeys, k)
        }
        sort.Strings(firstKeys) // deterministic order from first record
        for _, k := range firstKeys {
            seen[k] = struct{}{}
            columns = append(columns, k)
        }
        // Add any keys from other records we didn't see in first; keep deterministic by sorting.
        extrasSet := map[string]struct{}{}
        for _, r := range records {
            for k := range r.Values {
                if _, ok := seen[k]; !ok {
                    extrasSet[k] = struct{}{}
                }
            }
        }
        if len(extrasSet) > 0 {
            extras := make([]string, 0, len(extrasSet))
            for k := range extrasSet {
                extras = append(extras, k)
            }
            sort.Strings(extras)
            for _, k := range extras {
                seen[k] = struct{}{}
                columns = append(columns, k)
            }
        }
    }

    nc := len(columns)
    if nc == 0 {
        return ""
    }

    // Alignment: right-align a column iff ALL non-empty values are Int or Float.
    rightAlign := make([]bool, nc)
    for i := range rightAlign {
        rightAlign[i] = true // assume numeric until proven otherwise
    }

    // Prepare processed cells per row/col depending on overflow option (wrap/truncate).
    // For Wrap: each cell becomes []string lines.
    // For Truncate: each cell becomes a single []string{line}.
    type cellLines = []string
    tableCells := make([][]cellLines, len(records)) // rows -> cols -> lines

    // Widths start from header names (capped by MaxWidth).
    widths := make([]int, nc)
    for i, col := range columns {
        w := runeLen(col)
        if w > opts.MaxWidth {
            w = opts.MaxWidth
        }
        widths[i] = w
    }

    // Scan data to determine alignment and widths (consider wrapped lines).
    for ri, r := range records {
        row := make([]cellLines, nc)
        for ci, col := range columns {
            val, ok := r.Values[col]
            var raw string
            if ok && val.Kind() != Invalid {
                raw = val.String()
                switch val.Kind() {
                case Int, Float:
                    // keep column as numeric unless we find a non-numeric later
                default:
                    rightAlign[ci] = false
                }
            } else {
                // treat missing as non-numeric
                rightAlign[ci] = false
            }

            lines := makeCellLines(raw, opts.MaxWidth, opts.Overflow)
            row[ci] = lines

            // Update width from each produced line, capped at MaxWidth.
            for _, ln := range lines {
                l := runeLen(ln)
                if l > opts.MaxWidth {
                    l = opts.MaxWidth
                }
                if l > widths[ci] {
                    widths[ci] = l
                }
            }
        }
        tableCells[ri] = row
    }

    var b strings.Builder

    writeSep := func() {
        b.WriteByte('+')
        for i := 0; i < nc; i++ {
            for j := 0; j < widths[i]+2; j++ { // +2 for padding spaces
                b.WriteByte('-')
            }
            b.WriteByte('+')
        }
        b.WriteByte('\n')
    }

    writeRowLine := func(cells []string, alignRight []bool) {
        b.WriteByte('|')
        for i := 0; i < nc; i++ {
            cell := ""
            if i < len(cells) {
                cell = cells[i]
            }
            w := widths[i]
            if alignRight != nil && alignRight[i] {
                padding := w - runeLen(cell)
                if padding < 0 {
                    padding = 0
                }
                b.WriteByte(' ')
                b.WriteString(strings.Repeat(" ", padding))
                b.WriteString(cell)
                b.WriteByte(' ')
            } else {
                b.WriteByte(' ')
                b.WriteString(cell)
                padding := w - runeLen(cell)
                if padding < 0 {
                    padding = 0
                }
                b.WriteString(strings.Repeat(" ", padding))
                b.WriteByte(' ')
            }
            b.WriteByte('|')
        }
        b.WriteByte('\n')
    }

    // Top border
    writeSep()
    // Header (never wrapped; we may truncate header labels visually to MaxWidth)
    headerCells := make([]string, nc)
    for i, col := range columns {
        headerCells[i] = truncateToRunes(col, opts.MaxWidth)
    }
    writeRowLine(headerCells, nil)
    // Header/body separator
    writeSep()

    // Data rows: if wrapping, render multiple lines per record (row height = max cell lines).
    for _, row := range tableCells {
        maxLines := 1
        for _, cell := range row {
            if len(cell) > maxLines {
                maxLines = len(cell)
            }
        }
        for lineIdx := 0; lineIdx < maxLines; lineIdx++ {
            lineCells := make([]string, nc)
            for ci := 0; ci < nc; ci++ {
                if lineIdx < len(row[ci]) {
                    lineCells[ci] = strings.Trim(row[ci][lineIdx], `"`)
                } else {
                    lineCells[ci] = ""
                }
            }
            writeRowLine(lineCells, rightAlign)
        }
    }
    // Bottom border
    writeSep()

    return b.String()
}

// ---- Helpers ----

func makeCellLines(s string, maxWidth int, mode OverflowMode) []string {
    if s == "" {
        return []string{""}
    }
    switch mode {
    case Wrap:
        return wrapRunes(s, maxWidth)
    default: // Truncate
        return []string{truncateToRunes(s, maxWidth)}
    }
}

// wrapRunes wraps a string by rune count (no word-aware wrapping) to lines of length <= maxWidth.
func wrapRunes(s string, maxWidth int) []string {
    if maxWidth <= 0 {
        return []string{""}
    }
    rs := []rune(s)
    if len(rs) <= maxWidth {
        return []string{s}
    }
    var lines []string
    for start := 0; start < len(rs); start += maxWidth {
        end := start + maxWidth
        if end > len(rs) {
            end = len(rs)
        }
        lines = append(lines, string(rs[start:end]))
    }
    return lines
}

// truncateToRunes returns at most maxWidth runes.
func truncateToRunes(s string, maxWidth int) string {
    if maxWidth <= 0 {
        return ""
    }
    if utf8.RuneCountInString(s) <= maxWidth {
        return s
    }
    rs := []rune(s)
    return string(rs[:maxWidth])
}

func runeLen(s string) int { return utf8.RuneCountInString(s) }
