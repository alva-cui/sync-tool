package transform

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
)

type Engine struct {
	programs map[string]*vm.Program
	filter   *vm.Program
	mu       sync.RWMutex
	env      map[string]any
}

// NewEngine 初始化映射引擎（线程安全）
func NewEngine() *Engine {
	return &Engine{
		programs: make(map[string]*vm.Program),
		env:      SafeEnv(),
	}
}

// Compile 预编译所有映射规则与过滤表达式（任务启动时调用一次）
func (e *Engine) Compile(cfg Config) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// 1. 编译过滤器
	if cfg.Filter != "" {
		prog, err := expr.Compile(cfg.Filter, expr.Env(e.env), expr.AsBool())
		if err != nil {
			return fmt.Errorf("compile filter: %w", err)
		}
		e.filter = prog
	} else {
		e.filter = nil
	}

	// 2. 编译映射规则
	e.programs = make(map[string]*vm.Program)
	for _, rule := range cfg.Rules {
		exprStr := rule.Expr
		if exprStr == "" && rule.SourceCol != "" {
			exprStr = rule.SourceCol // 直连映射降级
		}
		if exprStr == "" {
			continue
		}

		prog, err := expr.Compile(exprStr, expr.Env(e.env), expr.Optimize(true))
		if err != nil {
			return fmt.Errorf("compile mapping '%s': %w", rule.TargetCol, err)
		}
		e.programs[rule.TargetCol] = prog
	}
	return nil
}

// Execute 逐行执行：过滤 → 映射 → 返回结果
// 返回值: (目标行, 是否通过过滤, 错误)
func (e *Engine) Execute(ctx context.Context, row map[string]any) (map[string]any, bool, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// 检查过滤条件
	if e.filter != nil {
		val, err := expr.Run(e.filter, row)
		if err != nil {
			return nil, false, fmt.Errorf("filter exec: %w", err)
		}
		if passed, ok := val.(bool); ok && !passed {
			return nil, false, nil // 过滤拦截，不报错
		}
	}

	// 执行字段映射
	result := make(map[string]any, len(e.programs))
	for col, prog := range e.programs {
		val, err := expr.Run(prog, row)
		if err != nil {
			// 记录上下文便于 DLQ 路由
			slog.WarnContext(ctx, "mapping eval failed", "col", col, "err", err, "row_keys", keys(row))
			return nil, false, fmt.Errorf("mapping '%s': %w", col, err)
		}
		result[col] = val
	}
	return result, true, nil
}

func keys(m map[string]any) []string {
	k := make([]string, 0, len(m))
	for key := range m {
		k = append(k, key)
	}
	return k
}
