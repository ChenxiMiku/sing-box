package inbound

import (
	"context"
	"os"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
)

var _ adapter.InboundManager = (*Manager)(nil)

type Manager struct {
	registry     adapter.InboundRegistry
	endpoint     adapter.EndpointManager
	access       sync.Mutex
	scope        *adapter.Scope
	inbounds     []adapter.Inbound
	inboundByTag map[string]adapter.Inbound
}

func NewManager(registry adapter.InboundRegistry, endpoint adapter.EndpointManager) *Manager {
	return &Manager{
		registry:     registry,
		endpoint:     endpoint,
		inboundByTag: make(map[string]adapter.Inbound),
	}
}

func (m *Manager) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	m.access.Lock()
	m.scope = scope
	inbounds := m.inbounds
	m.access.Unlock()
	for _, inbound := range inbounds {
		name := "inbound/" + inbound.Type() + "[" + inbound.Tag() + "]"
		err := scope.Start(name, inbound, stage)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) Close() error {
	m.access.Lock()
	inbounds := m.inbounds
	scope := m.scope
	m.inbounds = nil
	m.inboundByTag = make(map[string]adapter.Inbound)
	m.access.Unlock()
	if scope != nil {
		var err error
		for _, inbound := range inbounds {
			err = E.Errors(err, scope.CloseChild(inbound))
		}
		return err
	}
	return nil
}

func (m *Manager) Remove(tag string) error {
	m.access.Lock()
	inbound, found := m.inboundByTag[tag]
	if !found {
		m.access.Unlock()
		return os.ErrInvalid
	}
	delete(m.inboundByTag, tag)
	index := common.Index(m.inbounds, func(it adapter.Inbound) bool {
		return it == inbound
	})
	if index != -1 {
		m.inbounds = append(m.inbounds[:index], m.inbounds[index+1:]...)
	}
	scope := m.scope
	m.access.Unlock()
	if scope != nil {
		return scope.CloseChild(inbound)
	}
	return nil
}

func (m *Manager) Inbounds() []adapter.Inbound {
	m.access.Lock()
	defer m.access.Unlock()
	return m.inbounds
}

func (m *Manager) Get(tag string) (adapter.Inbound, bool) {
	m.access.Lock()
	inbound, found := m.inboundByTag[tag]
	m.access.Unlock()
	if found {
		return inbound, true
	}
	return m.endpoint.Get(tag)
}

func (m *Manager) Create(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, outboundType string, options any) error {
	inbound, err := m.registry.Create(ctx, router, logger, tag, outboundType, options)
	if err != nil {
		return err
	}
	m.access.Lock()
	scope := m.scope
	if existsInbound, loaded := m.inboundByTag[tag]; loaded {
		if scope != nil {
			_ = scope.CloseChild(existsInbound)
		}
		existsIndex := common.Index(m.inbounds, func(it adapter.Inbound) bool {
			return it == existsInbound
		})
		if existsIndex != -1 {
			m.inbounds = append(m.inbounds[:existsIndex], m.inbounds[existsIndex+1:]...)
		}
	}
	m.inbounds = append(m.inbounds, inbound)
	m.inboundByTag[tag] = inbound
	m.access.Unlock()
	if scope != nil {
		name := "inbound/" + inbound.Type() + "[" + inbound.Tag() + "]"
		for _, stage := range adapter.ListStartStages {
			err = scope.Start(name, inbound, stage)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
