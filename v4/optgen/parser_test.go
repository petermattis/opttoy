package optgen

import (
	"fmt"
	"strings"
	"testing"
)

var _ = fmt.Println

func TestParser(t *testing.T) {
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
		(Join $r:* $s:* & (IsLower $s $r))
		=>
		(Join $s $r)

		[EliminateVariable]
		(Eq $left:^(Variable) $right:(Variable)) => (Eq $right $left)
	`

	r := strings.NewReader(s)
	p := NewParser(r)
	root, err := p.Parse()
	if err != nil {
		t.Fatal(err)
	}

	fmt.Printf("%v\n", root)
}
