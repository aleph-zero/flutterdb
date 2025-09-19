package physical

import (
    "context"
    "errors"
    "fmt"
    "github.com/aleph-zero/flutterdb/engine"
    "github.com/aleph-zero/flutterdb/engine/ast"
    "github.com/aleph-zero/flutterdb/engine/token"
    "golang.org/x/exp/constraints"
    "math"
)

type FilterOperator struct {
    child     OperatorNode
    predicate ast.ExpressionNode
    evaluator *PredicateEvaluator
    source    <-chan *engine.Result
    sink      chan *engine.Result
}

func NewFilterOperator(child OperatorNode, predicate ast.ExpressionNode) *FilterOperator {
    return &FilterOperator{
        child:     child,
        predicate: predicate,
        evaluator: NewPredicateEvaluator(),
        source:    child.Sink(),
        sink:      make(chan *engine.Result),
    }
}

func (operator *FilterOperator) Sink() <-chan *engine.Result {
    return operator.sink
}

func (operator *FilterOperator) Accept(ctx context.Context, visitor OperatorNodeVisitor) error {
    return visitor.VisitFilterOperator(ctx, operator)
}

func (operator *FilterOperator) Open(ctx context.Context) error {
    go func() {
        defer close(operator.sink)
        for result := range operator.source {
            r, err := operator.filter(result.Record)
            if err != nil {
                // TODO XXX SIGNAL ERROR UPSTREAM
            }
            if r {
                operator.evaluator.stack.Clear()
                operator.sink <- result
            }
        }
    }()
    return nil
}

func (operator *FilterOperator) filter(record *engine.Record) (bool, error) {
    operator.evaluator.record = record
    if err := operator.predicate.Accept(operator.evaluator); err != nil {
        return false, err
    }

    final := operator.evaluator.stack.MustPop()
    if operator.evaluator.stack.Len() != 0 {
        panic("filter evaluator stack not empty")
    }

    fmt.Printf("final value: %v\n", final.String())
    fmt.Printf("final value kind: %v\n", final.Kind())
    fmt.Printf("final truth value: %t\n", final.ToBoolean())

    return final.ToBoolean(), nil
}

type PredicateEvaluator struct {
    record *engine.Record
    stack  *engine.Stack[*engine.Value]
}

func NewPredicateEvaluator() *PredicateEvaluator {
    return &PredicateEvaluator{
        stack: engine.NewStack[*engine.Value](),
    }
}

func (pe *PredicateEvaluator) comparable() bool {
    return true
}

func (pe *PredicateEvaluator) VisitBinaryExpressionNode(node *ast.BinaryExpressionNode) error {
    if err := node.Left.Accept(pe); err != nil {
        return err
    }
    if err := node.Right.Accept(pe); err != nil {
        return err
    }

    r := pe.stack.MustPop()
    l := pe.stack.MustPop()

    fmt.Printf("values after popping stack left: %v, right: %v\n", l.String(), r.String())

    switch node.Op.TokenType {
    case token.EQUAL, token.NOT_EQUAL, token.GT, token.GTE, token.LT, token.LTE:
        pe.stack.Push(comparison(l, r, node.Op.TokenType))
    case token.PLUS, token.MINUS, token.ASTERISK, token.DIVIDE, token.MODULO:
        v, err := arithmetic(l, r, node.Op.TokenType)
        if err != nil {
            return err
        }
        pe.stack.Push(v)
    case token.LIKE:
        panic("like unimplemented")
    case token.AND:
        v := engine.NewBooleanValue(l.ToBoolean() && r.ToBoolean())
        pe.stack.Push(&v)
    case token.OR:
        v := engine.NewBooleanValue(l.ToBoolean() || r.ToBoolean())
        pe.stack.Push(&v)
    default:
        panic("unimplemented binary operator")
    }
    return nil
}

func like(left, right *engine.Value) (*engine.Value, error) {

    return nil, nil
}

func arithmetic(left, right *engine.Value, op token.TokenType) (*engine.Value, error) {
    if left.CanInt() && right.CanInt() {
        if op == token.MODULO {
            v := engine.NewIntValue(left.ToInt() % right.ToInt())
            return &v, nil
        }
        v := engine.NewIntValue(apply(left.MustInt(), right.MustInt(), op))
        return &v, nil
    }

    l := left.ToFloat()
    r := right.ToFloat()
    if (op == token.DIVIDE || op == token.MODULO) && r == 0 {
        return nil, errors.New("division by zero")
    }
    if op == token.MODULO {
        v := engine.NewFloatValue(math.Mod(l, r))
        return &v, nil
    }

    v := engine.NewFloatValue(apply(left.ToFloat(), right.ToFloat(), op))
    return &v, nil
}

type calculable interface {
    constraints.Integer | constraints.Float
}

func apply[T calculable](left, right T, op token.TokenType) T {
    switch op {
    case token.PLUS:
        return left + right
    case token.MINUS:
        return left - right
    case token.ASTERISK:
        return left * right
    case token.DIVIDE:
        return left / right
    default:
        panic(fmt.Sprintf("cannot apply arithmetic operator '%s'", op.String()))
    }
}

func comparison(left, right *engine.Value, op token.TokenType) *engine.Value {
    if left.Kind() == engine.String && right.Kind() == engine.String {
        return compare(left.MustString(), right.MustString(), op)
    }

    if left.CanInt() && right.CanInt() {
        return compare(left.ToInt(), right.ToInt(), op)
    }

    return compare(left.ToFloat(), right.ToFloat(), op)
}

func compare[T constraints.Ordered](left, right T, op token.TokenType) *engine.Value {
    switch op {
    case token.EQUAL:
        v := engine.NewBooleanValue(left == right)
        return &v
    case token.NOT_EQUAL:
        v := engine.NewBooleanValue(left != right)
        return &v
    case token.GT:
        v := engine.NewBooleanValue(left > right)
        return &v
    case token.GTE:
        v := engine.NewBooleanValue(left >= right)
        return &v
    case token.LT:
        v := engine.NewBooleanValue(left < right)
        return &v
    case token.LTE:
        v := engine.NewBooleanValue(left <= right)
        return &v
    default:
        panic("unhandled default case")
    }
}

func (pe *PredicateEvaluator) VisitColumnIdentifierNode(node *ast.ColumnIdentifierNode) error {
    value, ok := pe.record.Values[node.Value]
    if !ok {
        panic(fmt.Sprintf("no value for column '%s' in record", node.Value))
    }
    pe.stack.Push(&value)
    return nil
}

func (pe *PredicateEvaluator) VisitIntegerLiteralNode(node *ast.IntegerLiteralNode) error {
    v := engine.NewIntValue(node.Value)
    pe.stack.Push(&v)
    return nil
}

func (pe *PredicateEvaluator) VisitFloatLiteralNode(node *ast.FloatLiteralNode) error {
    v := engine.NewFloatValue(node.Value)
    pe.stack.Push(&v)
    return nil
}

func (pe *PredicateEvaluator) VisitStringLiteralNode(node *ast.StringLiteralNode) error {
    v := engine.NewStringValue(node.Value)
    pe.stack.Push(&v)
    return nil
}

func (pe *PredicateEvaluator) VisitUnaryExpressionNode(node *ast.UnaryExpressionNode) error {
    if err := node.Node.Accept(pe); err != nil {
        return err
    }

    value := pe.stack.MustPop()
    if node.Op.TokenType != token.MINUS {
        return fmt.Errorf("unexpected operator type '%s'", node.Op.TokenType)
    }

    switch value.Kind() {
    case engine.Float:
        v := engine.NewFloatValue(-1 * value.MustFloat())
        pe.stack.Push(&v)
    case engine.Int:
        v := engine.NewIntValue(-1 * value.MustInt())
        pe.stack.Push(&v)
    default:
        return fmt.Errorf("improper value type for negation '%s'", value.Kind())
    }
    return nil
}

func (pe *PredicateEvaluator) VisitLogicalNegationNode(node *ast.LogicalNegationNode) error {
    if err := node.Node.Accept(pe); err != nil {
        return err
    }
    v := engine.NewBooleanValue(!pe.stack.MustPop().ToBoolean())
    pe.stack.Push(&v)
    return nil
}

func (pe *PredicateEvaluator) VisitParenthesizedExpression(node *ast.ParenthesizedExpressionNode) error {
    return node.Node.Accept(pe)
}

func (pe *PredicateEvaluator) VisitPredicateNode(node *ast.PredicateNode) error {
    return fmt.Errorf("cannot evaluate node type '%T'", node)
}

func (pe *PredicateEvaluator) VisitAsteriskLiteralNode(node *ast.AsteriskLiteralNode) error {
    return fmt.Errorf("cannot evaluate node type '%T'", node)
}

func (pe *PredicateEvaluator) VisitLimitNode(node *ast.LimitNode) error {
    return fmt.Errorf("cannot evaluate node type '%T'", node)
}

func (pe *PredicateEvaluator) VisitSelectStatementNode(node *ast.SelectStatementNode) error {
    return fmt.Errorf("cannot evaluate node type '%T'", node)
}

func (pe *PredicateEvaluator) VisitCreateTableStatementNode(node *ast.CreateTableStatementNode) error {
    return fmt.Errorf("cannot evaluate node type '%T'", node)
}

func (pe *PredicateEvaluator) VisitColumnDefinitionNode(node *ast.ColumnDefinitionNode) error {
    return fmt.Errorf("cannot evaluate node type '%T'", node)
}

func (pe *PredicateEvaluator) VisitTableIdentifierNode(node *ast.TableIdentifierNode) error {
    return fmt.Errorf("cannot evaluate node type '%T'", node)
}
