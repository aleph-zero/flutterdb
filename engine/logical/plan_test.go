package logical

import (
	"fmt"
	"github.com/aleph-zero/flutterdb/engine"
	"github.com/aleph-zero/flutterdb/engine/parser"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPlan_NewLogicalQueryPlan_CreateTable(t *testing.T) {
	teardown, store := setupSuite(t, data)
	defer teardown(t)

	tests := []struct {
		stmt string
	}{
		{`CREATE TABLE t ( c1 TEXT, c2 KEYWORD, c3 INTEGER, c4 DATETIME, c5 GEOPOINT )`},
		{`CREATE TABLE t (c1 TEXT, c2 KEYWORD, c3 INTEGER, c4 DATETIME, c5 GEOPOINT) PARTITION BY c3`},
	}

	for _, tt := range tests {
		t.Run(tt.stmt, func(t *testing.T) {
			tokens, err := parser.LexicalScan(tt.stmt)
			if err != nil {
				t.Fatalf("lexical error: %v", err)
			}

			p := parser.New(tokens)
			ast, err := p.Parse()
			if err != nil {
				t.Fatalf("%s", err)
			}

			_, err = engine.ResolveSymbols(store, ast)
			if err != nil {
				t.Fatalf("%s", err)
			}

			plan, err := NewQueryPlan(ast)
			if err != nil {
				t.Fatalf("%s", err)
			}

			fmt.Printf("created logical plan: %v+\n", plan)
		})
	}
}

func TestPlan_NewLogicalQueryPlan_Select(t *testing.T) {
	teardown, store := setupSuite(t, data)
	defer teardown(t)

	tests := []struct {
		stmt string
	}{
		{`SELECT 5`},
		{`SELECT "a"`},
		{`SELECT c1 FROM t1`},
		{`SELECT c1 FROM t1 LIMIT 5`},
		{`SELECT c1 * 2.5 FROM t1`},
		{`SELECT (c1) FROM t1`},
		{`SELECT c1, c2 FROM t1 WHERE c3 = 1`},
		{`SELECT c1, c2 FROM t1 WHERE c3 = 1 LIMIT 5`},
	}

	for _, tt := range tests {
		t.Run(tt.stmt, func(t *testing.T) {
			tokens, err := parser.LexicalScan(tt.stmt)
			if err != nil {
				t.Fatalf("lexical error: %v", err)
			}

			p := parser.New(tokens)
			ast, err := p.Parse()
			if err != nil {
				t.Fatalf("%s", err)
			}

			_, err = engine.ResolveSymbols(store, ast)
			if err != nil {
				t.Fatalf("%s", err)
			}

			plan, err := NewQueryPlan(ast)
			if err != nil {
				t.Fatalf("%s", err)
			}

			fmt.Printf("created logical plan: %v+\n", plan)
		})
	}
}
func TestPlan_NewLogicalQueryPlan_InvalidCreateTable(t *testing.T) {
	teardown, store := setupSuite(t, data)
	defer teardown(t)

	tests := []struct {
		stmt string
	}{
		{`CREATE TABLE t ( c1 TEXT, c2 KEYWORD, c3 INTEGER ) PARTITION BY c6`}, // partition non-existent column
	}

	for _, tt := range tests {
		t.Run(tt.stmt, func(t *testing.T) {
			tokens, err := parser.LexicalScan(tt.stmt)
			if err != nil {
				t.Fatalf("lexical error: %v", err)
			}

			p := parser.New(tokens)
			ast, err := p.Parse()
			if err != nil {
				t.Fatalf("%s", err)
			}

			_, err = engine.ResolveSymbols(store, ast)
			require.Error(t, err)
		})
	}
}
