package cdc

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

type MySQLBinlog struct {
	cfg     *canal.Config
	canal   *canal.Canal
	eventCh chan *Event
	mu      sync.Mutex
	lastPos mysql.Position
}

func NewMySQLBinlog(cfg SourceConfig) *MySQLBinlog {
	c := canal.NewDefaultConfig()
	c.Addr = fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	c.User = cfg.User
	c.Password = cfg.Pass
	c.Flavor = mysql.MySQLFlavor
	c.Dump.TableDB = cfg.DBName
	c.IncludeTableRegex = []string{fmt.Sprintf(`^%s\.%s$`, cfg.DBName, cfg.Table)}
	c.HeartbeatPeriod = 30 * time.Second

	return &MySQLBinlog{
		cfg:     c,
		eventCh: make(chan *Event, cfg.ChanSize),
	}
}

func (m *MySQLBinlog) Start(ctx context.Context, initialPos string) error {
	var err error
	m.canal, err = canal.NewCanal(m.cfg)
	if err != nil {
		return fmt.Errorf("init canal: %w", err)
	}
	m.canal.SetEventHandler(m)

	if initialPos != "" {
		name, posVal := splitPos(initialPos)
		if name != "" {
			m.lastPos = mysql.Position{Name: name, Pos: posVal}
			slog.Info("cdc initial pos recorded", "pos", initialPos)
		}
	}

	go func() {
		<-ctx.Done()
		if m.canal != nil {
			m.canal.Close()
		}
	}()

	return m.canal.Run()
}

// ✅ OnRow: NO EventHeader - 核心数据回调 (类型: *canal.RowsEvent)
func (m *MySQLBinlog) OnRow(e *canal.RowsEvent) error {
	if e == nil || len(e.Rows) == 0 {
		return nil
	}

	var evtType EventType
	switch e.Action {
	case canal.InsertAction:
		evtType = EventInsert
	case canal.UpdateAction:
		evtType = EventUpdate
	case canal.DeleteAction:
		evtType = EventDelete
	default:
		return nil
	}

	cols := e.Table.Columns
	for _, row := range e.Rows {
		data := make(map[string]any, len(cols))
		for i, col := range cols {
			data[col.Name] = row[i]
		}

		evt := &Event{
			Type:      evtType,
			Database:  e.Table.Schema,
			Table:     e.Table.Name,
			Data:      data,
			Cursor:    fmt.Sprintf("%s:%d", m.lastPos.Name, m.lastPos.Pos),
			Timestamp: time.Now().UTC(),
		}

		select {
		case m.eventCh <- evt:
		default:
			slog.Warn("cdc channel full", "table", evt.Table, "type", evtType)
		}
	}
	return nil
}

// ✅ OnPosSynced: HAS EventHeader - 第三个参数是 mysql.GTIDSet (interface)
func (m *MySQLBinlog) OnPosSynced(_ *replication.EventHeader, pos mysql.Position, _ mysql.GTIDSet, _ bool) error {
	m.mu.Lock()
	m.lastPos = pos
	m.mu.Unlock()
	return nil
}

// ✅ OnRotate: HAS EventHeader - []byte 转 string/uint32
func (m *MySQLBinlog) OnRotate(_ *replication.EventHeader, rotateEvent *replication.RotateEvent) error {
	m.mu.Lock()
	m.lastPos = mysql.Position{
		Name: string(rotateEvent.NextLogName),
		Pos:  uint32(rotateEvent.Position),
	}
	m.mu.Unlock()
	return nil
}

// ✅ OnDDL: HAS EventHeader - 第二个参数是 nextPos
func (m *MySQLBinlog) OnDDL(_ *replication.EventHeader, nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	slog.Info("ddl detected",
		"schema", string(queryEvent.Schema),
		"query", string(queryEvent.Query),
		"pos", fmt.Sprintf("%s:%d", nextPos.Name, nextPos.Pos),
	)
	return nil
}

// ✅ OnXID: HAS EventHeader
func (m *MySQLBinlog) OnXID(_ *replication.EventHeader, _ mysql.Position) error {
	return nil
}

// ✅ OnGTID: HAS EventHeader - 第二个参数是 mysql.BinlogGTIDEvent
func (m *MySQLBinlog) OnGTID(_ *replication.EventHeader, _ mysql.BinlogGTIDEvent) error {
	return nil
}

// ✅ OnTableChanged: HAS EventHeader
func (m *MySQLBinlog) OnTableChanged(_ *replication.EventHeader, schema, table string) error {
	return nil
}

// ✅ OnRowsQueryEvent: NO EventHeader
func (m *MySQLBinlog) OnRowsQueryEvent(e *replication.RowsQueryEvent) error {
	_ = e
	return nil
}

// ✅ OnTableNotFound: HAS EventHeader - 新增必需方法 (之前遗漏!)
func (m *MySQLBinlog) OnTableNotFound(_ *replication.EventHeader, e *replication.RowsEvent) error {
	if e != nil && e.Table != nil {
		// 🛠️ 修复: TableMapEvent 的字段是 Table (表名) 和 Schema (库名)
		// 且 replication.RowsEvent 没有 Action 字段
		slog.Warn("table not found in binlog",
			"schema", e.Table.Schema,
			"table", e.Table.Table, // ✅ 正确字段名
		)
	}
	return nil
}

// ✅ String: 必需方法
func (m *MySQLBinlog) String() string { return "mysql-binlog" }

// 辅助函数
func splitPos(pos string) (string, uint32) {
	parts := strings.SplitN(pos, ":", 2)
	if len(parts) != 2 {
		return "", 0
	}
	p, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return parts[0], 0
	}
	return parts[0], uint32(p)
}

// ✅ 在 MySQLBinlog 结构体末尾添加公共方法（暴露事件通道）
// Events 返回只读事件通道，供外部消费
func (m *MySQLBinlog) Events() <-chan *Event {
	return m.eventCh
}

func (m *MySQLBinlog) Close() error {
	if m.canal != nil {
		m.canal.Close()
		m.canal = nil
	}
	if m.eventCh != nil {
		close(m.eventCh)
	}
	return nil
}
