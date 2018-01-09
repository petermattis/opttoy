package optgen

import (
	"fmt"
	"strings"
	"testing"
)

var _ = fmt.Println

func TestCompilerTag(t *testing.T) {
	testCompiler(t,
		`
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
		(Join $r:* $s:*)
		=>
		$r
		`,
		`
		(Rules
			(Rule
				Header=(RuleHeader Name="NormalizeJoin" Tags=(Tags))
				Match=(MatchFields
					Names=InnerJoinOp
					(Bind Label="r" Target=(MatchAny))
					(Bind Label="s" Target=(MatchAny))
				)
				Replace=(Ref Label="r")
			)
			(Rule
				Header=(RuleHeader Name="NormalizeJoin" Tags=(Tags))
				Match=(MatchFields
					Names=LeftJoinOp
					(Bind Label="r" Target=(MatchAny))
					(Bind Label="s" Target=(MatchAny))
				)
				Replace=(Ref Label="r")
			)
		)
		`)
}

func TestCompilerOpNameArg(t *testing.T) {
	testCompiler(t,
		`
		define InnerJoin {
			Left  Expr
			Right Expr
		}

		[NormalizeJoin]
		(InnerJoin $r:* $s:*)
		=>
		(Do (OpName) $s $r)
		`,
		`
		(Rule
			Header=(RuleHeader Name="NormalizeJoin" Tags=(Tags))
			Match=(MatchFields
				Names=InnerJoinOp
				(Bind Label="r" Target=(MatchAny))
				(Bind Label="s" Target=(MatchAny))
			)
			Replace=(Construct
				OpName="Do"
				InnerJoinOp
				(Ref Label="s")
				(Ref Label="r")
			)
		)
		`)
}

func testCompiler(t *testing.T, in, expected string) {
	r := strings.NewReader(in)
	c := NewCompiler(r)
	compiled, err := c.Compile()
	if err != nil {
		t.Fatal(err)
	}

	if testing.Verbose() {
		fmt.Printf("%s\n=>\n\n%s\n", in, compiled.String())
	}

	if !strings.Contains(removeWhitespace(compiled.String()), removeWhitespace(expected)) {
		t.Fatalf("\nexpected:\n%s\nactual:\n%s", expected, compiled.String())
	}
}
