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
    s.elements[n-1] = zero // help GC when T holds pointers
    s.elements = s.elements[:n-1]
    return v, true
}

func (s *Stack[T]) Len() int {
    return len(s.elements)
}

func (s *Stack[T]) MustPop() T {
    n := len(s.elements)
    if n == 0 {
        panic("attempt to pop empty stack")
    }

    v := s.elements[n-1]
    var zero T
    s.elements[n-1] = zero
    s.elements = s.elements[:n-1]
    return v
}

// Clear keeps capacity (fast reuse) and zeroes elements for GC.
func (s *Stack[T]) Clear() {
    // zero elements to allow GC
    for i := range s.elements {
        var zero T
        s.elements[i] = zero
    }
    s.elements = s.elements[:0]
}

// Reset frees the backing array (releases memory).
func (s *Stack[T]) Reset() {
    s.elements = nil
}
