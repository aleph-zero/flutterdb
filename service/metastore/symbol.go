package metastore

import "github.com/aleph-zero/flutterdb/engine/types"

type SymbolTable struct {
	TableScopeSymbols map[string]TableScopeSymbolTableEntry
}

func NewSymbolTable() *SymbolTable {
	return &SymbolTable{TableScopeSymbols: make(map[string]TableScopeSymbolTableEntry)}
}

type TableScopeSymbolTableEntry struct {
	TableName          string
	ColumnScopeSymbols []ColumnScopeSymbolTableEntry
}

type ColumnScopeSymbolTableEntry struct {
	TableName  string
	ColumnName string
	ColumnType types.Type
}

func (s SymbolTable) GetTableNames() []string {
	names := make([]string, 0, len(s.TableScopeSymbols))
	for k, _ := range s.TableScopeSymbols {
		names = append(names, k)
	}
	return names
}
