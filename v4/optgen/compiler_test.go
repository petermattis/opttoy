package main

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var _ = fmt.Println

func TestCompiler(t *testing.T) {
	s := `
		define Lt {
			Left  Expr
			Right Expr
		}

		define Int {
			Private Int
		}

		[NormalizeLt]
		(Lt $left:(Int) $right:*)
		=>
		(Gt $right $left)

		[NormalizeJoin]
		(Join $r:* $s:* && (IsLower $s $r))
		=>
		(Join $s $r)
	`

	r := strings.NewReader(s)
	c := NewCompiler(r)
	opt, err := c.Compile()
	require.NoError(t, err)

	fmt.Printf("%v\n", opt)
}
