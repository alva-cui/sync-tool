package cdc

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

// PGConfig PostgreSQL CDC 配置
type PGConfig struct {
	Host     string
	Port     uint16
	User     string
	Pass     string
	DBName   string
	Table    string // 支持 "schema.table" 格式
	SlotName string // 逻辑复制槽名称，默认 "sync_tool_<table>"
	ChanSize int
	Driver   string // 用于工厂模式识别
}

// RelationInfo 缓存表结构元数据
type RelationInfo struct {
	Schema       string
	Name         string
	Columns      []ColumnMeta
	ColNameByIdx map[int]string // 索引 → 列名（加速查找）
}

type ColumnMeta struct {
	Name         string
	DataType     uint32 // PostgreSQL OID
	TypeModifier int32
	NotNull      bool
}

// PGReplication PostgreSQL Logical Replication 实现
type PGReplication struct {
	cfg         PGConfig
	pgConn      *pgconn.PgConn
	conn        *pgx.Conn
	eventCh     chan *Event
	mu          sync.Mutex
	lastLSN     pglogrepl.LSN
	typeMap     *pgtype.Map
	relations   map[uint32]*RelationInfo // RelationID → 表结构
	standbyDone chan struct{}
}

func NewPGReplication(cfg PGConfig) *PGReplication {
	slot := cfg.SlotName
	if slot == "" {
		slot = fmt.Sprintf("sync_tool_%s", strings.ReplaceAll(cfg.Table, ".", "_"))
	}
	cfg.SlotName = slot

	return &PGReplication{
		cfg:         cfg,
		eventCh:     make(chan *Event, cfg.ChanSize),
		typeMap:     pgtype.NewMap(),
		relations:   make(map[uint32]*RelationInfo),
		standbyDone: make(chan struct{}),
	}
}

func (p *PGReplication) Start(ctx context.Context, initialLSN string) error {
	connCfg, err := pgx.ParseConfig(fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		p.cfg.Host, p.cfg.Port, p.cfg.User, p.cfg.Pass, p.cfg.DBName,
	))
	if err != nil {
		return fmt.Errorf("parse pg config: %w", err)
	}
	connCfg.RuntimeParams = map[string]string{
		"replication": "database",
	}

	conn, err := pgx.ConnectConfig(ctx, connCfg)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	p.conn = conn
	p.pgConn = conn.PgConn()

	if initialLSN != "" {
		p.lastLSN, err = pglogrepl.ParseLSN(initialLSN)
		if err != nil {
			p.close()
			return fmt.Errorf("parse initial LSN: %w", err)
		}
	}

	if err := p.ensureReplicationSlot(ctx); err != nil {
		p.close()
		return err
	}

	pluginArgs := []string{
		"proto_version '1'",
		fmt.Sprintf("publication_names 'sync_tool_pub'"),
	}
	if err := pglogrepl.StartReplication(
		ctx, p.pgConn, p.cfg.SlotName, p.lastLSN,
		pglogrepl.StartReplicationOptions{PluginArgs: pluginArgs},
	); err != nil {
		p.close()
		return fmt.Errorf("start replication: %w", err)
	}

	go p.runDecoder(ctx)
	return nil
}

func (p *PGReplication) ensureReplicationSlot(ctx context.Context) error {
	_, err := p.conn.Exec(ctx, fmt.Sprintf(
		`CREATE PUBLICATION IF NOT EXISTS sync_tool_pub FOR TABLE %s`,
		p.cfg.Table,
	))
	if err != nil {
		return fmt.Errorf("create publication: %w", err)
	}

	var exists bool
	err = p.conn.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_replication_slots WHERE slot_name = $1
		)
	`, p.cfg.SlotName).Scan(&exists)
	if err != nil {
		return fmt.Errorf("check slot: %w", err)
	}

	if !exists {
		_, err = pglogrepl.CreateReplicationSlot(
			ctx, p.pgConn, p.cfg.SlotName, "pgoutput",
			pglogrepl.CreateReplicationSlotOptions{},
		)
		if err != nil {
			return fmt.Errorf("create slot: %w", err)
		}
		slog.Info("created replication slot", "slot", p.cfg.SlotName)
	}
	return nil
}

func (p *PGReplication) runDecoder(ctx context.Context) {
	defer p.close()

	clientXLogPos := p.lastLSN
	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msgCtx, cancel := context.WithDeadline(ctx, nextStandbyMessageDeadline)
		rawMsg, err := p.pgConn.ReceiveMessage(msgCtx)
		cancel()

		if err != nil {
			if pgconn.Timeout(err) || err == context.DeadlineExceeded {
				if err := p.sendStandbyStatus(clientXLogPos); err != nil {
					slog.Warn("send standby status failed", "err", err)
				}
				nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
				continue
			}
			slog.Error("receive message failed", "err", err)
			return
		}

		switch msg := rawMsg.(type) {
		case *pgproto3.CopyData:
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					slog.Warn("parse keepalive", "err", err)
					continue
				}
				if pkm.ReplyRequested {
					_ = p.sendStandbyStatus(clientXLogPos)
				}
				if pkm.ServerWALEnd > clientXLogPos {
					clientXLogPos = pkm.ServerWALEnd
				}

			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					slog.Warn("parse xlog data", "err", err)
					continue
				}

				events, err := p.decodeLogicalMessage(xld.WALData)
				if err != nil {
					slog.Warn("decode logical message", "err", err)
					continue
				}

				for _, evt := range events {
					evt.Cursor = clientXLogPos.String()
					select {
					case p.eventCh <- evt:
					default:
						slog.Warn("PG CDC channel full", "table", evt.Table)
					}
				}

				clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
				nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
			}
		}
	}
}

func (p *PGReplication) decodeLogicalMessage(walData []byte) ([]*Event, error) {
	logicalMsg, err := pglogrepl.Parse(walData)
	if err != nil {
		return nil, fmt.Errorf("parse logical msg: %w", err)
	}

	var events []*Event

	switch msg := logicalMsg.(type) {
	case *pglogrepl.RelationMessage:
		// ✅ 缓存表结构：msg.Columns 是 []RelationMessageColumn
		rel := &RelationInfo{
			Schema:       msg.Namespace,
			Name:         msg.RelationName,
			Columns:      make([]ColumnMeta, len(msg.Columns)),
			ColNameByIdx: make(map[int]string),
		}
		for i, col := range msg.Columns {
			rel.Columns[i] = ColumnMeta{
				Name:         col.Name,
				DataType:     col.DataType,
				TypeModifier: col.TypeModifier,
				NotNull:      col.Flags != 0,
			}
			rel.ColNameByIdx[i] = col.Name
		}
		p.mu.Lock()
		p.relations[msg.RelationID] = rel
		p.mu.Unlock()
		slog.Debug("cached relation", "id", msg.RelationID, "table", rel.Name)

	case *pglogrepl.BeginMessage:
		// 忽略

	case *pglogrepl.InsertMessage:
		rel := p.getRelation(msg.RelationID)
		if rel == nil || !p.matchTable(rel.Name) {
			break
		}
		data := p.decodeTuple(rel, msg.Tuple)
		events = append(events, &Event{
			Type:      EventInsert,
			Database:  rel.Schema,
			Table:     rel.Name,
			Data:      data,
			Timestamp: time.Now().UTC(),
		})

	case *pglogrepl.UpdateMessage:
		rel := p.getRelation(msg.RelationID)
		if rel == nil || !p.matchTable(rel.Name) {
			break
		}
		tuple := msg.NewTuple
		if tuple == nil {
			tuple = msg.OldTuple
		}
		data := p.decodeTuple(rel, tuple)
		events = append(events, &Event{
			Type:      EventUpdate,
			Database:  rel.Schema,
			Table:     rel.Name,
			Data:      data,
			Timestamp: time.Now().UTC(),
		})

	case *pglogrepl.DeleteMessage:
		rel := p.getRelation(msg.RelationID)
		if rel == nil || !p.matchTable(rel.Name) {
			break
		}
		data := p.decodeTuple(rel, msg.OldTuple)
		events = append(events, &Event{
			Type:      EventDelete,
			Database:  rel.Schema,
			Table:     rel.Name,
			Data:      data,
			Timestamp: time.Now().UTC(),
		})

	case *pglogrepl.CommitMessage:
		// 忽略
	}

	return events, nil
}

func (p *PGReplication) getRelation(id uint32) *RelationInfo {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.relations[id]
}

// decodeTuple: ✅ 关键修复 - TupleDataColumn 没有 Name 字段，需通过索引查 Relation
func (p *PGReplication) decodeTuple(rel *RelationInfo, tuple *pglogrepl.TupleData) map[string]any {
	if rel == nil || tuple == nil {
		return nil
	}
	data := make(map[string]any, len(tuple.Columns))

	for colIdx, col := range tuple.Columns {
		// ✅ TupleDataColumn 结构: {DataType byte, Data []byte}
		// DataType: 'n'=null, 'u'=unchanged toast, 't'=text value
		if col.DataType == 'n' {
			// null 值，跳过或设为 nil
			colName, ok := rel.ColNameByIdx[colIdx]
			if ok {
				data[colName] = nil
			}
			continue
		}
		if col.DataType == 'u' {
			// unchanged toast，跳过
			continue
		}

		// ✅ 通过列索引从 Relation 获取元数据
		if colIdx >= len(rel.Columns) {
			continue
		}
		colMeta := rel.Columns[colIdx]
		colName := colMeta.Name

		data[colName] = p.decodeColumnValue(colMeta.DataType, col.Data)
	}
	return data
}

// decodeColumnValue: 将 PG binary 值解码为 Go 类型
func (p *PGReplication) decodeColumnValue(oid uint32, data []byte) any {
	if data == nil {
		return nil
	}

	// 尝试 pgtype 解码
	var val any
	if err := p.typeMap.SQLScanner(&val).Scan(data); err == nil {
		return normalizePGValue(val)
	}

	// 回退：按 OID 硬编码解码
	switch oid {
	case pgtype.TextOID, pgtype.VarcharOID, pgtype.BPCharOID, pgtype.JSONBOID, pgtype.JSONOID:
		return string(data)
	case pgtype.Int2OID:
		if len(data) == 2 {
			return int16(binary.BigEndian.Uint16(data))
		}
	case pgtype.Int4OID:
		if len(data) == 4 {
			return int32(binary.BigEndian.Uint32(data))
		}
	case pgtype.Int8OID:
		if len(data) == 8 {
			return int64(binary.BigEndian.Uint64(data))
		}
	case pgtype.Float4OID:
		if len(data) == 4 {
			bits := binary.BigEndian.Uint32(data)
			return math.Float32frombits(bits)
		}
	case pgtype.Float8OID:
		if len(data) == 8 {
			bits := binary.BigEndian.Uint64(data)
			return math.Float64frombits(bits)
		}
	case pgtype.BoolOID:
		return len(data) > 0 && data[0] == 't'
	case pgtype.TimestampOID, pgtype.TimestamptzOID:
		// PG timestamp: microseconds since 2000-01-01
		if len(data) == 8 {
			micros := int64(binary.BigEndian.Uint64(data))
			return time.UnixMicro(micros + 946684800000000).UTC()
		}
	case pgtype.DateOID:
		// PG date: days since 2000-01-01
		if len(data) == 4 {
			days := int32(binary.BigEndian.Uint32(data))
			return time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(days)).UTC()
		}
	}
	// 未知类型返回 string
	return string(data)
}

func normalizePGValue(v any) any {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case time.Time:
		return val.UTC()
	case []byte:
		return string(val)
	default:
		return val
	}
}

func (p *PGReplication) matchTable(tableName string) bool {
	if p.cfg.Table == "" {
		return true
	}
	if strings.Contains(p.cfg.Table, ".") {
		return tableName == p.cfg.Table
	}
	parts := strings.Split(tableName, ".")
	if len(parts) == 2 {
		return parts[1] == p.cfg.Table
	}
	return tableName == p.cfg.Table
}

func (p *PGReplication) sendStandbyStatus(pos pglogrepl.LSN) error {
	return pglogrepl.SendStandbyStatusUpdate(context.Background(), p.pgConn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: pos,
		WALFlushPosition: pos,
		WALApplyPosition: pos,
		ClientTime:       time.Now(),
	})
}

func (p *PGReplication) Events() <-chan *Event {
	return p.eventCh
}

func (p *PGReplication) Close() error {
	p.close()
	return nil
}

func (p *PGReplication) close() {
	if p.conn != nil {
		_ = p.conn.Close(context.Background())
		p.conn = nil
		p.pgConn = nil
	}
	select {
	case <-p.standbyDone:
	default:
		close(p.standbyDone)
	}
}

func (p *PGReplication) String() string { return "pg-logical-replication" }
