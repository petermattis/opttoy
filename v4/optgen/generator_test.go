package main

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var _ = fmt.Println

func TestOpGen(t *testing.T) {
	s := `
		define Lt {
			Left  Expr
			Right Expr
		}

		define Int {
			Value Int
		}

		define True {}
	`

	r := strings.NewReader(s)
	c := NewCompiler(r)
	root, err := c.Compile()
	require.NoError(t, err)

	var buf bytes.Buffer
	gen := NewGenerator("myopt", root)
	err = gen.GenerateOps(&buf)
	require.NoError(t, err)

	fmt.Println(buf.String())
}

func TestExprGen(t *testing.T) {
	s := `
		define Lt {
			Left  Expr
			Right Expr
		}

		define Int {
			Value Int
		}

		define True {}
	`

	r := strings.NewReader(s)
	c := NewCompiler(r)
	root, err := c.Compile()
	require.NoError(t, err)

	var buf bytes.Buffer
	gen := NewGenerator("myopt", root)
	err = gen.GenerateExprs(&buf)
	require.NoError(t, err)

	fmt.Println(buf.String())
}

func TestFactoryGen(t *testing.T) {
	s := `
		define Select {
			Input  Expr
			Filter Expr
		}

		define InnerJoin {
			Left   Expr
			Right  Expr
			Filter Expr
		}

		define And {
			Left  Expr
			Right Expr
		}

		define Int {
			Value Int
		}

		[MergeSelectWithInnerJoin]
		(Select
			(InnerJoin $r:* $s:* $inner:*)
			$outer:*
		) =>
		(InnerJoin $r $s (And $inner $outer))
	`

	r := strings.NewReader(s)
	c := NewCompiler(r)
	root, err := c.Compile()
	require.NoError(t, err)

	var buf bytes.Buffer
	gen := NewGenerator("myopt", root)
	err = gen.GenerateFactory(&buf)
	require.NoError(t, err)

	fmt.Println(buf.String())
}
