package parser

import (
    "fmt"
    "github.com/aleph-zero/flutterdb/engine/ast"
    "github.com/aleph-zero/flutterdb/engine/token"
    "github.com/aleph-zero/flutterdb/engine/types"
    "strconv"
)

/*
   statement                -> select_statement
                            | create_table_statement
   select_statement         -> 'SELECT' projections ('FROM' IDENTIFIER)? ('WHERE' disjunction)? ('LIMIT' INTEGER)?
   projections              -> disjunction (',' disjunction)*
                            | '*'
   disjunction              -> conjunction ('OR' conjunction)*
   conjunction              -> equality ('AND' equality)*
   negation                 -> ('NOT')* equality
   equality                 -> comparison (('!=' | '=' | 'LIKE') comparison)*
   comparison               -> term (('>' | '>=' | '<' | '<=') term)*
   term                     -> factor (('-' | '+') factor)*
   factor                   -> unary (('/' | '*' | '%') unary)*
   unary                    -> ('-')? unary
                            | primary ;
   primary                  -> INTEGER|FLOAT|STRING|IDENTIFIER
                            | '(' disjunction ')' ;

   create_table_statement   -> 'CREATE' 'TABLE' IDENTIFIER '(' columns ')' ('PARTITION BY' IDENTIFIER)?
   columns                  -> column_definition (',' column_definition)*
   column_definition        -> IDENTIFIER ('TEXT'|'KEYWORD'|'INTEGER'|'FLOAT'|'GEOPOINT'|'DATETIME')
*/

type Parser struct {
    tokens []token.Token
    index  int
}

func New(tokens []token.Token) *Parser {
    return &Parser{
        tokens: tokens,
        index:  0,
    }
}

// Parse returns an abstract syntax tree representing the logical structure of
// the provided SQL statement.
func (p *Parser) Parse() (ast.VisitableNode, error) {
    return p.statement()
}

func (p *Parser) statement() (ast.VisitableNode, error) {
    switch {
    case p.match(token.SELECT):
        return p.selectStatement()
    case p.match(token.CREATE):
        if !p.match(token.TABLE) {
            return nil, ParseError{
                Expected: []token.TokenType{token.TABLE},
                Received: p.peek(),
            }
        }
        return p.createTableStatement()
    default:
        return nil, ParseError{
            Expected: []token.TokenType{token.SELECT, token.CREATE},
            Received: p.peek(),
        }
    }
}

func (p *Parser) createTableStatement() (ast.VisitableNode, error) {
    if !p.match(token.IDENTIFIER) {
        return nil, ParseError{
            Expected: []token.TokenType{token.IDENTIFIER},
            Received: p.peek(),
        }
    }

    name := p.previous()
    if !p.match(token.L_PAREN) {
        return nil, ParseError{
            Expected: []token.TokenType{token.L_PAREN},
            Received: p.peek(),
        }
    }

    var cds []ast.VisitableNode
    for ok := true; ok; ok = p.match(token.COMMA) {
        cd, err := p.columnDefinition()
        if err != nil {
            return nil, err
        }
        cds = append(cds, cd)
    }

    if !p.match(token.R_PAREN) {
        return nil, ParseError{
            Expected: []token.TokenType{token.R_PAREN},
            Received: p.peek(),
        }
    }

    var partition string
    if p.match(token.PARTITION) {
        if !p.match(token.BY) {
            return nil, ParseError{
                Expected: []token.TokenType{token.BY},
                Received: p.peek(),
            }
        }
        if !p.match(token.IDENTIFIER) {
            return nil, ParseError{
                Expected: []token.TokenType{token.IDENTIFIER},
                Received: p.peek(),
            }
        }
        id, err := p.identifier()
        if err != nil {
            return nil, ParseError{
                Expected: []token.TokenType{token.IDENTIFIER},
                Received: p.peek(),
            }
        }
        partition = id.(*ast.ColumnIdentifierNode).Value
    }

    if !p.eof() {
        return nil, ParseError{
            Expected: []token.TokenType{token.EOF},
            Received: p.peek(),
        }
    }

    return ast.NewCreateTableStatementNode(name.Lexeme, cds, partition), nil
}

func (p *Parser) columnDefinition() (ast.VisitableNode, error) {
    if !p.match(token.IDENTIFIER) {
        return nil, ParseError{
            Expected: []token.TokenType{token.IDENTIFIER},
            Received: p.peek(),
        }
    }

    name := p.previous()
    if !p.match(token.TEXT, token.KEYWORD, token.INTEGER, token.FLOAT, token.GEOPOINT, token.DATETIME) {
        return nil, ParseError{
            Expected: []token.TokenType{token.TEXT, token.KEYWORD, token.INTEGER, token.FLOAT, token.GEOPOINT, token.DATETIME},
            Received: p.peek(),
        }
    }

    tok := p.previous()
    t, err := types.New(tok.Lexeme)
    if err != nil {
        return nil, ConversionError{
            Value: tok,
            err:   err,
        }
    }

    return ast.NewColumnDefinitionNode(name.Lexeme, t), nil
}

func (p *Parser) selectStatement() (ast.VisitableNode, error) {
    var expressions []ast.ExpressionNode

    switch {
    case p.match(token.ASTERISK):
        expressions = append(expressions, ast.NewAsteriskLiteralNode())
        if p.check(token.COMMA) {
            // TODO - Add LIMIT, ORDER BY as possible next tokens
            return nil, ParseError{
                Expected: []token.TokenType{token.FROM, token.WHERE},
                Received: p.peek(),
            }
        }
    default:
        for ok := true; ok; ok = p.match(token.COMMA) {
            expr, err := p.disjunction()
            if err != nil {
                return nil, err
            }
            expressions = append(expressions, expr)
        }
    }

    stmt := ast.NewSelectStatementNode(expressions)
    if p.match(token.FROM) {
        if !p.match(token.IDENTIFIER) {
            return nil, ParseError{
                Expected: []token.TokenType{token.IDENTIFIER},
                Received: p.peek(),
            }
        }
        tok := p.previous()
        stmt.Table = ast.NewTableIdentifierNode(tok.Lexeme)
    }

    if p.match(token.WHERE) {
        predicate, err := p.disjunction()
        if err != nil {
            return nil, err
        }
        stmt.Predicate = ast.NewPredicateNode(predicate)
    }

    if p.match(token.LIMIT) {
        if !p.match(token.INTEGER) {
            return nil, ParseError{
                Expected: []token.TokenType{token.INTEGER},
                Received: p.peek(),
            }
        }
        limit, err := p.integer()
        if err != nil {
            return nil, err
        }
        stmt.Limit = ast.NewLimitNode(*(limit.(*ast.IntegerLiteralNode)))
    }

    if !p.eof() {
        return nil, ParseError{
            Expected: []token.TokenType{token.EOF},
            Received: p.peek(),
        }
    }

    return stmt, nil
}

func (p *Parser) disjunction() (ast.ExpressionNode, error) {
    expr, err := p.conjunction()
    if err != nil {
        return nil, err
    }

    for p.match(token.OR) {
        op := p.previous()
        right, err := p.conjunction()
        if err != nil {
            return nil, err
        }
        expr = ast.NewBinaryExpressionNode(op, expr, right)
    }

    return expr, nil
}

func (p *Parser) conjunction() (ast.ExpressionNode, error) {
    expr, err := p.negation()
    if err != nil {
        return nil, err
    }

    for p.match(token.AND) {
        op := p.previous()
        right, err := p.negation()
        if err != nil {
            return nil, err
        }
        expr = ast.NewBinaryExpressionNode(op, expr, right)
    }

    return expr, nil
}

func (p *Parser) negation() (ast.ExpressionNode, error) {
    if p.match(token.NOT) {
        op := p.previous()
        node, err := p.negation()
        if err != nil {
            return nil, err
        }
        return ast.NewLogicalNegationNode(op, node), nil
    }

    return p.equality()
}

func (p *Parser) equality() (ast.ExpressionNode, error) {
    expr, err := p.comparison()
    if err != nil {
        return nil, err
    }

    for p.match(token.EQUAL, token.NOT_EQUAL, token.LIKE) {
        op := p.previous()
        right, err := p.comparison()
        if err != nil {
            return nil, err
        }
        expr = ast.NewBinaryExpressionNode(op, expr, right)
    }

    return expr, nil
}

func (p *Parser) comparison() (ast.ExpressionNode, error) {
    expr, err := p.term()
    if err != nil {
        return nil, err
    }

    for p.match(token.GT, token.GTE, token.LT, token.LTE) {
        op := p.previous()
        right, err := p.term()
        if err != nil {
            return nil, err
        }
        expr = ast.NewBinaryExpressionNode(op, expr, right)
    }

    return expr, nil
}

func (p *Parser) term() (ast.ExpressionNode, error) {
    expr, err := p.factor()
    if err != nil {
        return nil, err
    }

    for p.match(token.PLUS, token.MINUS) {
        op := p.previous()
        right, err := p.factor()
        if err != nil {
            return nil, err
        }
        expr = ast.NewBinaryExpressionNode(op, expr, right)
    }

    return expr, nil
}

func (p *Parser) factor() (ast.ExpressionNode, error) {
    expr, err := p.unary()
    if err != nil {
        return nil, err
    }

    for p.match(token.DIVIDE, token.ASTERISK, token.MODULO) {
        op := p.previous()
        right, err := p.unary()
        if err != nil {
            return nil, err
        }
        expr = ast.NewBinaryExpressionNode(op, expr, right)
    }

    return expr, nil
}

func (p *Parser) unary() (ast.ExpressionNode, error) {
    if p.match(token.MINUS) {
        op := p.previous()
        node, err := p.unary()
        if err != nil {
            return nil, err
        }
        return ast.NewUnaryExpressionNode(op, node), nil
    }

    return p.primary()
}

func (p *Parser) primary() (ast.ExpressionNode, error) {
    switch {
    case p.match(token.INTEGER):
        return p.integer()
    case p.match(token.FLOAT):
        return p.float()
    case p.match(token.IDENTIFIER):
        return p.identifier()
    case p.match(token.STRING):
        return p.string()
    case p.match(token.L_PAREN):
        expr, err := p.disjunction()
        if err != nil {
            return nil, err
        }
        if !p.check(token.R_PAREN) {
            return nil, ParseError{
                Expected: []token.TokenType{token.R_PAREN},
                Received: p.peek(),
            }
        }
        p.advance()
        return ast.NewParenthesizedExpressionNode(expr), nil
    default:
        return nil, ParseError{
            Expected: []token.TokenType{token.INTEGER, token.FLOAT, token.STRING, token.IDENTIFIER},
            Received: p.peek(),
        }
    }
}

func (p *Parser) integer() (ast.ExpressionNode, error) {
    tok := p.previous()
    value, err := strconv.ParseInt(tok.Lexeme, 10, 64)
    if err != nil {
        return nil, ConversionError{
            Value: tok,
            err:   err,
        }
    }
    return ast.NewIntegerLiteralNode(value), nil
}

func (p *Parser) float() (ast.ExpressionNode, error) {
    tok := p.previous()
    value, err := strconv.ParseFloat(tok.Lexeme, 64)
    if err != nil {
        return nil, ConversionError{
            Value: tok,
            err:   err,
        }
    }
    return ast.NewFloatLiteralNode(value), nil
}

func (p *Parser) identifier() (ast.ExpressionNode, error) {
    tok := p.previous()
    return ast.NewColumnIdentifierNode(tok.Lexeme), nil
}

func (p *Parser) string() (ast.ExpressionNode, error) {
    tok := p.previous()
    return ast.NewStringLiteralNode(tok.Lexeme), nil
}

/** Helper Methods **/

func (p *Parser) match(tokenTypes ...token.TokenType) bool {
    for _, tokenType := range tokenTypes {
        if p.check(tokenType) {
            p.advance()
            return true
        }
    }
    return false
}

func (p *Parser) check(tokenType token.TokenType) bool {
    if p.eof() {
        return false
    }
    return p.peek().TokenType == tokenType
}

func (p *Parser) advance() token.Token {
    if !p.eof() {
        p.index++
    }
    return p.previous()
}

func (p *Parser) previous() token.Token {
    return p.tokens[p.index-1]
}

func (p *Parser) peek() token.Token {
    return p.tokens[p.index]
}

func (p *Parser) eof() bool {
    return p.peek().TokenType == token.EOF
}

/** Error Handling **/

type ParseError struct {
    Expected []token.TokenType
    Received token.Token
}

func (e ParseError) Error() string {
    return fmt.Sprintf("parser expected one of '%s' received '%s' at line: %d, column: %d",
        e.Expected, e.Received.Lexeme, e.Received.Position.Line, e.Received.Position.Column)
}

type ConversionError struct {
    Value token.Token
    err   error
}

func (e ConversionError) Error() string {
    return fmt.Sprintf("parser cannot convert token '%s' to concrete type: %s",
        e.Value.Lexeme, e.err)
}

func (e ConversionError) Unwrap() error {
    return e.err
}
