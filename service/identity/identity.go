package identity

type Service interface {
    Identify() string
}

type ServiceProvider struct {
    identity string
}

func NewService(id string) Service {
    return &ServiceProvider{identity: id}
}

func (sl ServiceProvider) Identify() string {
    return sl.identity
}

type Model struct {
    Identity string `json:"identity"`
}
