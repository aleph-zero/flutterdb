package ast

type Visitor interface {
    VisitSelectStatementNode(*SelectStatementNode) error
    VisitPredicateNode(*PredicateNode) error
    VisitCreateTableStatementNode(*CreateTableStatementNode) error
    VisitShowTablesStatementNode(*ShowTablesStatementNode) error
    VisitColumnDefinitionNode(*ColumnDefinitionNode) error

    VisitTableIdentifierNode(*TableIdentifierNode) error
    VisitColumnIdentifierNode(*ColumnIdentifierNode) error

    VisitParenthesizedExpression(*ParenthesizedExpressionNode) error
    VisitLogicalNegationNode(*LogicalNegationNode) error
    VisitUnaryExpressionNode(*UnaryExpressionNode) error
    VisitBinaryExpressionNode(*BinaryExpressionNode) error

    VisitStringLiteralNode(*StringLiteralNode) error
    VisitIntegerLiteralNode(*IntegerLiteralNode) error
    VisitFloatLiteralNode(*FloatLiteralNode) error
    VisitAsteriskLiteralNode(*AsteriskLiteralNode) error

    VisitLimitNode(*LimitNode) error
}
