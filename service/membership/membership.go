package membership

import (
    "fmt"
    "github.com/hashicorp/serf/serf"
    "maps"
    "net"
    "slices"
    "sync"
)

type Membership struct {
    lock        sync.RWMutex
    members     map[string]*Member
    joinAddrs   []string
    localMember string
    serf        *serf.Serf
    events      chan serf.Event
}

func NewService(member *Member, joinAddrs []string) (*Membership, error) {
    m := &Membership{
        members:   make(map[string]*Member),
        joinAddrs: joinAddrs,
    }
    m.members[member.Name] = member
    m.localMember = member.Name

    if err := m.initialize(member); err != nil {
        return nil, err
    }
    return m, nil
}

func (m *Membership) Members() []*Member {
    m.lock.RLock()
    defer m.lock.RUnlock()
    return slices.Collect(maps.Values(m.members))
}

func (m *Membership) Add(member *Member) {
    m.lock.Lock()
    defer m.lock.Unlock()
    m.members[member.Name] = member
}

func (m *Membership) Remove(name string) {
    m.lock.Lock()
    defer m.lock.Unlock()
    delete(m.members, name)
}

type Member struct {
    Name string            `json:"name"`
    Addr string            `json:"addr"`
    Port uint16            `json:"port"`
    Tags map[string]string `json:"tags"`
}

func NewMember(name, addr string, port uint16) *Member {
    return &Member{
        Name: name,
        Addr: addr,
        Port: port,
    }
}

func (m *Membership) eventHandler() {
    for e := range m.events {
        switch e.EventType() {
        case serf.EventMemberJoin:
            for _, sm := range e.(serf.MemberEvent).Members {
                if m.serf.LocalMember().Name == sm.Name {
                    continue
                }
                fmt.Printf("i am local node: %s and a new node joined: %s (+%v)\n", m.serf.LocalMember().Name, sm.Name, sm)
                m.Add(NewMember(sm.Name, sm.Addr.String(), sm.Port))
            }
        case serf.EventMemberLeave, serf.EventMemberFailed:
            for _, sm := range e.(serf.MemberEvent).Members {
                if m.serf.LocalMember().Name == sm.Name {
                    continue
                }
                fmt.Printf("i am local node: %s and a new node left: %s (+%v)\n", m.serf.LocalMember().Name, sm.Name, sm)
                m.Remove(sm.Name)
            }
        }
    }
}

func (m *Membership) initialize(member *Member) error {
    addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", member.Addr, member.Port))
    if err != nil {
        return err
    }

    m.events = make(chan serf.Event)
    config := serf.DefaultConfig()
    config.Init()
    config.MemberlistConfig.BindAddr = addr.IP.String()
    config.MemberlistConfig.BindPort = addr.Port
    config.EventCh = m.events
    config.Tags = member.Tags
    config.NodeName = member.Name
    m.serf, err = serf.Create(config)
    if err != nil {
        return err
    }

    go m.eventHandler()
    if m.joinAddrs != nil {
        _, err := m.serf.Join(m.joinAddrs, true)
        if err != nil {
            return err
        }
    }
    return nil
}
