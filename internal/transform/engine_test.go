package transform

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEngine_CompileAndExecute(t *testing.T) {
	tests := []struct {
		name     string
		config   Config
		input    map[string]any
		wantOut  map[string]any
		wantPass bool
		wantErr  bool
	}{
		{
			name:     "direct_mapping",
			config:   Config{Rules: []MappingRule{{TargetCol: "user_id", SourceCol: "id"}}},
			input:    map[string]any{"id": 1001},
			wantOut:  map[string]any{"user_id": 1001},
			wantPass: true,
		},
		{
			name: "concat_and_coalesce",
			config: Config{Rules: []MappingRule{
				{TargetCol: "full_name", Expr: `concat(first_name, " ", coalesce(last_name, ""))`},
			}},
			input:    map[string]any{"first_name": "Alice", "last_name": nil},
			wantOut:  map[string]any{"full_name": "Alice "},
			wantPass: true,
		},
		{
			name:    "filter_reject",
			config:  Config{Filter: `status == "active" && age >= 18`},
			input:   map[string]any{"status": "inactive", "age": 20},
			wantOut: nil, wantPass: false, wantErr: false,
		},
		{
			name:    "compile_error",
			config:  Config{Rules: []MappingRule{{TargetCol: "bad", Expr: `undefined_func(x)`}}},
			wantErr: true,
		},
		{
			name: "phone_mask",
			config: Config{Rules: []MappingRule{
				{TargetCol: "phone", Expr: `mask_phone(phone)`},
			}},
			input:    map[string]any{"phone": "13812345678"},
			wantOut:  map[string]any{"phone": "138****5678"},
			wantPass: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine := NewEngine()

			if tt.config.Rules != nil || tt.config.Filter != "" {
				err := engine.Compile(tt.config)
				if tt.wantErr {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
			}

			ctx := context.Background()
			out, passed, err := engine.Execute(ctx, tt.input)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.wantPass, passed)
			if tt.wantPass {
				assert.Equal(t, tt.wantOut, out)
			}
		})
	}
}
