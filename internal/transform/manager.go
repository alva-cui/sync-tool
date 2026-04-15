package transform

import "sync"

// Manager 管理任务级转换引擎的生命周期
type Manager struct {
	mu      sync.RWMutex
	engines map[string]*Engine // key: task_id
}

func NewManager() *Manager {
	return &Manager{engines: make(map[string]*Engine)}
}

// CompileAndCache 预编译配置并缓存。若已存在则覆盖。
func (m *Manager) CompileAndCache(taskID string, cfg Config) error {
	engine := NewEngine()
	if err := engine.Compile(cfg); err != nil {
		return err
	}

	m.mu.Lock()
	m.engines[taskID] = engine
	m.mu.Unlock()
	return nil
}

// Get 获取已缓存的引擎。未编译或不存在返回 false。
func (m *Manager) Get(taskID string) (*Engine, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	eng, ok := m.engines[taskID]
	return eng, ok
}

// Invalidate 清理缓存（任务删除/配置变更时调用）
func (m *Manager) Invalidate(taskID string) {
	m.mu.Lock()
	delete(m.engines, taskID)
	m.mu.Unlock()
}
