package physical

import (
	"github.com/aleph-zero/flutterdb/engine"
	"github.com/aleph-zero/flutterdb/engine/logical"
	"github.com/aleph-zero/flutterdb/engine/parser"
	"github.com/aleph-zero/flutterdb/service/index"
	"github.com/aleph-zero/flutterdb/service/metastore"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
	"testing"
)

const data = "../../testdata/metastore"

func TestFilterOperator_Filter(t *testing.T) {
	teardown, metaSvc, indexSvc := setup(t, data)
	defer teardown(t)
	ctx := context.Background()

	tests := []struct {
		stmt   string
		record *engine.Record
	}{
		{`SELECT * FROM t1 WHERE c3 = 5`, recordWithValue("c3", engine.NewIntValue(1))}, // integer equality
		{`SELECT * FROM t1 WHERE c3 = c4`, nil},                                         // column equality
	}
	for _, tt := range tests {
		t.Run(tt.stmt, func(t *testing.T) {
			p := plan(t, metaSvc, indexSvc, tt.stmt)
			f := &FilterOperatorFinder{}
			p.RootOperator.Accept(ctx, f)
			require.NotNil(t, f.operator)
			require.True(t, f.operator.filter(tt.record))
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

func recordWithValue(name string, value engine.Value) *engine.Record {
	r := engine.NewRecord()
	r.AddValue(name, value)
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
