package engine

import (
	"fmt"
	"github.com/aleph-zero/flutterdb/engine/ast"
	"github.com/aleph-zero/flutterdb/service/metastore"
	"strings"
)

func ResolveSymbols(meta metastore.Service, root ast.VisitableNode) (*metastore.SymbolTable, error) {
	symbols := metastore.NewSymbolTable()
	r := TableIdentifierResolver{
		meta:        meta,
		SymbolTable: symbols}

	if err := root.Accept(&r); err != nil {
		return nil, fmt.Errorf("resolving table names: %w", err)
	}

	c := ColumnIdentifierResolver{symbols}
	if err := root.Accept(&c); err != nil {
		return nil, fmt.Errorf("resolving column names: %w", err)
	}

	return symbols, nil
}

/* *** Table Identifier Resolver *** */

type TableIdentifierResolver struct {
	meta        metastore.Service
	SymbolTable *metastore.SymbolTable
}

func (t *TableIdentifierResolver) VisitTableIdentifierNode(node *ast.TableIdentifierNode) error {
	table, err := t.meta.GetTable(node.Value)
	if err != nil {
		return err
	}

	if _, ok := t.SymbolTable.TableScopeSymbols[table.TableName]; !ok {
		columns := make([]metastore.ColumnScopeSymbolTableEntry, 0)
		for _, c := range table.Columns {
			columns = append(columns, metastore.ColumnScopeSymbolTableEntry{
				TableName:  table.TableName,
				ColumnName: c.ColumnName,
				ColumnType: c.ColumnType,
			})
		}

		entry := metastore.TableScopeSymbolTableEntry{
			TableName:          table.TableName,
			ColumnScopeSymbols: columns,
		}

		node.ResolvedTableSymbol = &entry
		t.SymbolTable.TableScopeSymbols[table.TableName] = entry
	}

	return nil
}

func (t *TableIdentifierResolver) VisitSelectStatementNode(node *ast.SelectStatementNode) error {
	return node.Table.Accept(t)
}

func (t *TableIdentifierResolver) VisitPredicateNode(*ast.PredicateNode) error { return nil }

func (t *TableIdentifierResolver) VisitCreateTableStatementNode(*ast.CreateTableStatementNode) error {
	return nil
}

func (t *TableIdentifierResolver) VisitColumnDefinitionNode(*ast.ColumnDefinitionNode) error {
	return nil
}

func (t *TableIdentifierResolver) VisitColumnIdentifierNode(*ast.ColumnIdentifierNode) error {
	return nil
}

func (t *TableIdentifierResolver) VisitExpressionNode(*ast.ExpressionNode) error { return nil }

func (t *TableIdentifierResolver) VisitParenthesizedExpression(*ast.ParenthesizedExpressionNode) error {
	return nil
}

func (t *TableIdentifierResolver) VisitLogicalNegationNode(*ast.LogicalNegationNode) error {
	return nil
}
func (t *TableIdentifierResolver) VisitUnaryExpressionNode(*ast.UnaryExpressionNode) error {
	return nil
}
func (t *TableIdentifierResolver) VisitBinaryExpressionNode(*ast.BinaryExpressionNode) error {
	return nil
}
func (t *TableIdentifierResolver) VisitStringLiteralNode(*ast.StringLiteralNode) error { return nil }
func (t *TableIdentifierResolver) VisitIntegerLiteralNode(node *ast.IntegerLiteralNode) error {
	return nil
}
func (t *TableIdentifierResolver) VisitFloatLiteralNode(node *ast.FloatLiteralNode) error {
	return nil
}
func (t *TableIdentifierResolver) VisitAsteriskLiteralNode(*ast.AsteriskLiteralNode) error {
	return nil
}
func (t *TableIdentifierResolver) VisitLimitNode(*ast.LimitNode) error { return nil }

/* *** Column Identifier Resolver *** */

type ColumnIdentifierResolver struct {
	SymbolTable *metastore.SymbolTable
}

func (c *ColumnIdentifierResolver) VisitSelectStatementNode(node *ast.SelectStatementNode) error {
	if len(node.Expressions) == 1 {
		if _, ok := node.Expressions[0].(*ast.AsteriskLiteralNode); ok {
			columns := make([]ast.ExpressionNode, 0)
			for _, tableScopeSymbol := range c.SymbolTable.TableScopeSymbols {
				for _, columnScopeSymbol := range tableScopeSymbol.ColumnScopeSymbols {
					columns = append(columns, ast.NewColumnIdentifierNode(columnScopeSymbol.ColumnName))
				}
			}
			node.Expressions = columns
		}
	}

	for _, expr := range node.Expressions {
		if err := expr.Accept(c); err != nil {
			return err
		}
	}
	return node.Predicate.Accept(c)
}

func (c *ColumnIdentifierResolver) VisitColumnIdentifierNode(node *ast.ColumnIdentifierNode) error {
	for _, entry := range c.SymbolTable.TableScopeSymbols {
		for _, columnScopeSymbol := range entry.ColumnScopeSymbols {
			if node.Value == columnScopeSymbol.ColumnName {
				node.ResolvedColumnSymbol = &columnScopeSymbol
			}
		}
	}

	if node.ResolvedColumnSymbol == nil {
		return fmt.Errorf("column '%s' does not exist in table list\n", node.Value)
	}
	return nil
}

func (c *ColumnIdentifierResolver) VisitAsteriskLiteralNode(*ast.AsteriskLiteralNode) error {
	return nil
}

func (c *ColumnIdentifierResolver) VisitBinaryExpressionNode(node *ast.BinaryExpressionNode) error {
	if err := node.Left.Accept(c); err != nil {
		return err
	}
	return node.Right.Accept(c)
}

func (c *ColumnIdentifierResolver) VisitStringLiteralNode(*ast.StringLiteralNode) error { return nil }
func (c *ColumnIdentifierResolver) VisitIntegerLiteralNode(node *ast.IntegerLiteralNode) error {
	return nil
}
func (c *ColumnIdentifierResolver) VisitFloatLiteralNode(node *ast.FloatLiteralNode) error {
	return nil
}
func (c *ColumnIdentifierResolver) VisitTableIdentifierNode(*ast.TableIdentifierNode) error {
	return nil
}

func (c *ColumnIdentifierResolver) VisitUnaryExpressionNode(node *ast.UnaryExpressionNode) error {
	return node.Node.Accept(c)
}

func (c *ColumnIdentifierResolver) VisitLogicalNegationNode(node *ast.LogicalNegationNode) error {
	return node.Node.Accept(c)
}

func (c *ColumnIdentifierResolver) VisitParenthesizedExpression(node *ast.ParenthesizedExpressionNode) error {
	return node.Node.Accept(c)
}

func (c *ColumnIdentifierResolver) VisitPredicateNode(node *ast.PredicateNode) error {
	return node.Node.Accept(c)
}

func (c *ColumnIdentifierResolver) VisitCreateTableStatementNode(node *ast.CreateTableStatementNode) error {
	if node.Partition == "" {
		return nil
	}
	found := false
	for _, cd := range node.ColumnDefinitions {
		cdn := cd.(*ast.ColumnDefinitionNode)
		if strings.EqualFold(cdn.Value, node.Partition) {
			found = true
		}
	}
	if !found {
		return fmt.Errorf("invalid partition column '%s' does not exist in column definition list", node.Partition)
	}
	return nil
}

func (c *ColumnIdentifierResolver) VisitColumnDefinitionNode(node *ast.ColumnDefinitionNode) error {
	return nil
}

func (c *ColumnIdentifierResolver) VisitLimitNode(node *ast.LimitNode) error { return nil }
