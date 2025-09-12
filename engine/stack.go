package engine

type Stack[T any] struct {
    elements []T
}

func NewStack[T any]() *Stack[T] {
    return &Stack[T]{elements: make([]T, 0)}
}

func (s *Stack[T]) Push(element T) {
    s.elements = append(s.elements, element)
}

func (s *Stack[T]) Pop() (v T, ok bool) {
    var zero T
    n := len(s.elements)
    if n == 0 {
        return zero, false
    }

    v = s.elements[n-1]
    s.elements[n-1] = zero
    s.elements = s.elements[:n-1]
    return v, true
}

func (s *Stack[T]) Len() int {
    return len(s.elements)
}
