package engine

import (
	"fmt"
	"github.com/aleph-zero/flutterdb/engine/parser"
	"github.com/aleph-zero/flutterdb/engine/types"
	"github.com/aleph-zero/flutterdb/service/metastore"
	"github.com/google/go-cmp/cmp"
	"testing"
)

const data = "../testdata/metastore"

func TestResolver_ValidSymbols(t *testing.T) {
	teardown, metaSvc := setupSuite(t, data)
	defer teardown(t)

	symbols := metastore.SymbolTable{
		TableScopeSymbols: map[string]metastore.TableScopeSymbolTableEntry{
			"t1": {"t1", []metastore.ColumnScopeSymbolTableEntry{
				{"t1", "c1", types.KEYWORD},
				{"t1", "c2", types.TEXT},
				{"t1", "c3", types.INTEGER},
				{"t1", "c4", types.FLOAT},
				{"t1", "c5", types.GEOPOINT},
				{"t1", "c6", types.DATETIME},
			}},
		}}

	tests := []struct {
		stmt string
		st   metastore.SymbolTable
	}{
		{`SELECT c1 FROM t1`, symbols},
		{`SELECT c2 FROM t1 WHERE c2 = 4`, symbols},
		{`SELECT c2 FROM t1 WHERE c4 = 4.5`, symbols},
		{`SELECT c1 FROM t1 WHERE c2 = 4 OR c3 = 'a'`, symbols},
		{`SELECT * FROM t1`, symbols},

		// TODO - Must also test for invalid comparisons, e.g. string > numeric
	}

	for _, tt := range tests {
		t.Run(tt.stmt, func(t *testing.T) {
			fmt.Println(">>> TESTING STATEMENT: ", tt.stmt)

			tokens, err := parser.LexicalScan(tt.stmt)
			if err != nil {
				t.Fatalf("lexical error: %v", err)
			}

			p := parser.New(tokens)
			ast, err := p.Parse()
			if err != nil {
				t.Fatalf("%s", err)
			}

			st, err := ResolveSymbols(metaSvc, ast)
			if err != nil {
				t.Fatalf("%s", err)
			}

			fmt.Printf("resolved symbols: %+v\n", st)
			fmt.Printf("test symbols:      %+v\n", tt.st)

			if diff := cmp.Diff(tt.st, *st); diff != "" {
				t.Errorf("symbol table mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestResolver_InvalidSymbols(t *testing.T) {
	teardown, store := setupSuite(t, data)
	defer teardown(t)

	tests := []struct {
		stmt string
	}{
		{`SELECT x FROM t1`},
		{`SELECT c5 FROM t2`},
		{`SELECT c1, x FROM t1`},
		{`SELECT c1 FROM t1 WHERE x = 5`},
	}

	for _, tt := range tests {
		t.Run(tt.stmt, func(t *testing.T) {
			fmt.Println(">>> TESTING STATEMENT: ", tt.stmt)
			tokens, err := parser.LexicalScan(tt.stmt)
			if err != nil {
				t.Fatalf("lexical error: %v", err)
			}

			p := parser.New(tokens)
			ast, err := p.Parse()
			if err != nil {
				t.Fatalf("%s", err)
			}

			_, err = ResolveSymbols(store, ast)
			if err == nil {
				t.Fatalf("expected error for invalid symbol")
			}
		})
	}
}

func setupSuite(tb testing.TB, testdata string) (func(tb testing.TB), metastore.Service) {
	ms := metastore.NewService(testdata)
	if err := ms.Open(); err != nil {
		tb.Fatal(err)
	}
	return func(tb testing.TB) { /* no-op teardown */ }, ms
}
