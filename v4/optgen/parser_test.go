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
			Left  expr
			Right expr
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

		[EliminateVariable]
		(Eq $left:^(Variable) $right:(Variable)) => (Eq $right $left)
	`

	r := strings.NewReader(s)
	c := NewParser(r)
	root, err := c.Parse()
	require.NoError(t, err)

	fmt.Printf("%v\n", root)
}
