package cluster

type Service interface {
    ClusterInfo() Model
}

type ServiceProvider struct {
    address string
    port    uint16
}

func (i ServiceProvider) ClusterInfo() Model {
    return Model{
        Address: i.address,
        Port:    i.port,
    }
}

func NewService(address string, port uint16) Service {
    return &ServiceProvider{
        address: address,
        port:    port,
    }
}

type Model struct {
    Address string `json:"address"`
    Port    uint16 `json:"port"`
}
