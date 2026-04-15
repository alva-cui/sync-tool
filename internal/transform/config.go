package transform

// MappingRule 定义目标字段的映射来源或计算表达式
type MappingRule struct {
	TargetCol string `json:"target_col"`
	SourceCol string `json:"source_col,omitempty"` // 直连字段（可选）
	Expr      string `json:"expr"`                 // 表达式（优先级高于 SourceCol）
}

// Config 任务级转换配置
type Config struct {
	Rules  []MappingRule `json:"mapping_rules"`
	Filter string        `json:"filter_expr,omitempty"` // 过滤条件，返回 bool
}
