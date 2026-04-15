package transform

import (
	"fmt"
	"strings"
	"time"
)

// SafeEnv 返回沙箱化表达式运行环境
func SafeEnv() map[string]any {
	return map[string]any{
		"concat": func(args ...any) string {
			var b strings.Builder
			for _, a := range args {
				fmt.Fprint(&b, a)
			}
			return b.String()
		},
		"coalesce": func(args ...any) any {
			for _, a := range args {
				if a != nil {
					return a
				}
			}
			return nil
		},
		"now": func() time.Time { return time.Now().UTC() },
		"mask_phone": func(phone string) string {
			if len(phone) < 7 {
				return "***"
			}
			return phone[:3] + "****" + phone[len(phone)-4:]
		},
		"to_int": func(v any) int64 {
			switch val := v.(type) {
			case float64:
				return int64(val)
			case int:
				return int64(val)
			case int64:
				return val
			case string:
				var n int64
				fmt.Sscanf(val, "%d", &n)
				return n
			default:
				return 0
			}
		},
	}
}
