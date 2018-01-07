package optgen

import (
	"fmt"
	"strings"
	"testing"
)

var _ = fmt.Println

func TestCompilerTag(t *testing.T) {
	s := `
		[Join]
		define InnerJoin {
			Left  Expr
			Right Expr
		}

		[Join]
		define LeftJoin {
			Left  Expr
			Right Expr
		}

		[NormalizeJoin]
		(Join $r:* $s:* & (IsLower $s $r))
		=>
		((OpName) $s $r)
	`

	r := strings.NewReader(s)
	c := NewCompiler(r)
	compiled, err := c.Compile()
	if err != nil {
		t.Fatal(err)
	}

	s2 := compiled.Rules()[0].String()
	fmt.Println(s2)
}

func TestCompilerOpNameArg(t *testing.T) {
	s := `
		define InnerJoin {
			Left  Expr
			Right Expr
		}

		[NormalizeJoin]
		(InnerJoin $r:* $s:* & (IsLower $s $r))
		=>
		(Do (OpName) $s $r)
	`

	r := strings.NewReader(s)
	c := NewCompiler(r)
	compiled, err := c.Compile()
	if err != nil {
		t.Fatal(err)
	}

	fmt.Printf("%v\n", compiled.Rules())
}
