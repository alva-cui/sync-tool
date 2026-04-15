package cdc

import "time"

type EventType string

const (
	EventInsert EventType = "insert"
	EventUpdate EventType = "update"
	EventDelete EventType = "delete"
)

// Event 标准 CDC 事件，供 transform 引擎消费
type Event struct {
	Type      EventType
	Database  string
	Table     string
	Data      map[string]any // After image（Delete 为 Before image）
	Cursor    string         // Binlog File:Pos 或 GTID
	Timestamp time.Time
}
