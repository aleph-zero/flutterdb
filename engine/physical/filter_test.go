package physical

import (
    "context"
    "github.com/aleph-zero/flutterdb/engine"
    "github.com/aleph-zero/flutterdb/engine/logical"
    "github.com/aleph-zero/flutterdb/engine/parser"
    "github.com/aleph-zero/flutterdb/service/index"
    "github.com/aleph-zero/flutterdb/service/metastore"
    "github.com/stretchr/testify/require"
    "testing"
)

const data = "../../testdata/metastore"

func TestFilterOperator_True(t *testing.T) {
    teardown, metaSvc, indexSvc := setup(t, data)
    defer teardown(t)
    ctx := context.Background()

    tests := []struct {
        stmt   string
        record *engine.Record
    }{
        {`SELECT * FROM t1 WHERE 1`, nil},
        {`SELECT * FROM t1 WHERE 1.0`, nil},
        {`SELECT * FROM t1 WHERE -1`, nil},
        {`SELECT * FROM t1 WHERE "1"`, nil},
        {`SELECT * FROM t1 WHERE NOT 0`, nil},
        {`SELECT * FROM t1 WHERE NOT NOT 1`, nil},
        {`SELECT * FROM t1 WHERE (1)`, nil},
        {`SELECT * FROM t1 WHERE (-1)`, nil},
        {`SELECT * FROM t1 WHERE 1 = 1`, nil},
        {
            `SELECT * FROM t1 WHERE c3 = 5`, //  integer-to-column equality
            recordWithValues(map[string]engine.Value{"c3": engine.NewIntValue(5)}),
        },
        {
            `SELECT * FROM t1 WHERE c3 = c4`, // column-to-column equality
            recordWithValues(map[string]engine.Value{"c3": engine.NewIntValue(4), "c4": engine.NewIntValue(4)}),
        },
        {
            `SELECT * FROM t1 WHERE c3 < (1 + c4)`, // column comparison w/sub-expression
            recordWithValues(map[string]engine.Value{"c3": engine.NewIntValue(5), "c4": engine.NewIntValue(5)}),
        },
        {`SELECT * FROM t1 WHERE "a" = "a"`, nil},
        {`SELECT * FROM t1 WHERE "a" < "z"`, nil},
        {`SELECT * FROM t1 WHERE  "1" = c3`,
            recordWithValues(map[string]engine.Value{"c3": engine.NewIntValue(1)})},
        {`SELECT * FROM t1 WHERE  "0" != c3`,
            recordWithValues(map[string]engine.Value{"c3": engine.NewIntValue(1)})},
        {`SELECT * FROM t1 WHERE  c3 > 5`,
            recordWithValues(map[string]engine.Value{"c3": engine.NewIntValue(10)})},
        {`SELECT * FROM t1 WHERE  c1 = "apple"`,
            recordWithValues(map[string]engine.Value{"c1": engine.NewStringValue("apple")})},
        {`SELECT * FROM t1 WHERE 1 = 1 AND "a" = "a"`, nil},
        {`SELECT * FROM t1 WHERE 1 = 1 AND NOT "a" != "a"`, nil},
        {`SELECT * FROM t1 WHERE 1 = 2 OR "a" = "a"`, nil},
        {`SELECT * FROM t1 WHERE (1 + 2) > c4`,
            recordWithValues(map[string]engine.Value{"c4": engine.NewFloatValue(2.5)})},
        {`SELECT * FROM t1 WHERE (1 + 2) > c3 AND (1.5 * 3) = c4`,
            recordWithValues(map[string]engine.Value{"c3": engine.NewIntValue(2), "c4": engine.NewFloatValue(4.5)})},
        {`SELECT * FROM t1 WHERE c1 > (c3 + (c4 * 2))`,
            recordWithValues(map[string]engine.Value{"c1": engine.NewStringValue("10"), "c3": engine.NewIntValue(2), "c4": engine.NewFloatValue(1.5)})},
        {`SELECT * FROM t1 WHERE c2 LIKE "%ppl%"`,
            recordWithValues(map[string]engine.Value{"c2": engine.NewStringValue("apple")})},
    }
    for _, tt := range tests {
        t.Run(tt.stmt, func(t *testing.T) {
            p := plan(t, metaSvc, indexSvc, tt.stmt)
            f := &FilterOperatorFinder{}
            p.RootOperator.Accept(ctx, f)
            require.NotNil(t, f.operator)
            r, err := f.operator.filter(tt.record)
            require.NoError(t, err)
            require.True(t, r)
        })
    }
}

func TestFilterOperator_False(t *testing.T) {
    teardown, metaSvc, indexSvc := setup(t, data)
    defer teardown(t)
    ctx := context.Background()

    tests := []struct {
        stmt   string
        record *engine.Record
    }{
        {`SELECT * FROM t1 WHERE 0`, nil},          // integer
        {`SELECT * FROM t1 WHERE -0`, nil},         // negative zero
        {`SELECT * FROM t1 WHERE NOT 1`, nil},      // integer negation
        {`SELECT * FROM t1 WHERE 1 = 2`, nil},      // false integer equality
        {`SELECT * FROM t1 WHERE "a"`, nil},        // false string false
        {`SELECT * FROM t1 WHERE "0"`, nil},        // false string as int
        {`SELECT * FROM t1 WHERE ""`, nil},         // false empty string
        {`SELECT * FROM t1 WHERE "a" != "a"`, nil}, // false string comparison
        {`SELECT * FROM t1 WHERE "a" > "z"`, nil},  // false string gt
        {`SELECT * FROM t1 WHERE  c1 = "apple"`,
            recordWithValues(map[string]engine.Value{"c1": engine.NewStringValue("pumpkin")})},
    }
    for _, tt := range tests {
        t.Run(tt.stmt, func(t *testing.T) {
            p := plan(t, metaSvc, indexSvc, tt.stmt)
            f := &FilterOperatorFinder{}
            p.RootOperator.Accept(ctx, f)
            r, err := f.operator.filter(tt.record)
            require.NoError(t, err)
            require.False(t, r)
        })
    }
}

type FilterOperatorFinder struct {
    operator *FilterOperator
}

func (f *FilterOperatorFinder) VisitFilterOperator(ctx context.Context, operator *FilterOperator) error {
    f.operator = operator
    return nil
}
func (f *FilterOperatorFinder) VisitLimitOperator(ctx context.Context, operator *LimitOperator) error {
    operator.child.Accept(ctx, f)
    return nil
}
func (f *FilterOperatorFinder) VisitProjectOperator(ctx context.Context, operator *ProjectOperator) error {
    operator.child.Accept(ctx, f)
    return nil
}
func (f *FilterOperatorFinder) VisitScanOperator(ctx context.Context, operator *ScanOperator) error {
    return nil
}
func (f *FilterOperatorFinder) VisitCreateOperator(ctx context.Context, operator *CreateOperator) error {
    return nil
}

func recordWithValues(values map[string]engine.Value) *engine.Record {
    r := engine.NewRecord()
    for k, v := range values {
        r.AddValue(k, v)
    }
    return r
}

func plan(t testing.TB, metaSvc metastore.Service, indexSvc index.Service, query string) *QueryPlan {
    tokens, err := parser.LexicalScan(query)
    require.NoError(t, err)
    root, err := parser.New(tokens).Parse()
    require.NoError(t, err)
    _, err = engine.ResolveSymbols(metaSvc, root)
    require.NoError(t, err)
    logicalPlan, err := logical.NewQueryPlan(root)
    require.NoError(t, err)
    physicalPlan, err := logical.OptimizeQueryPlan(logicalPlan)
    require.NoError(t, err)
    finalPlan, err := NewQueryPlan(metaSvc, indexSvc, physicalPlan)
    require.NoError(t, err)
    return finalPlan
}

func setup(tb testing.TB, testdata string) (func(tb testing.TB), metastore.Service, index.Service) {
    ms := metastore.NewService(testdata)
    if err := ms.Open(); err != nil {
        tb.Fatal(err)
    }
    return func(tb testing.TB) { /* no-op teardown */ }, ms, index.NewService(ms)
}
