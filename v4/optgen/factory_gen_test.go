package optgen

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

var _ = fmt.Println

func TestFactoryGenNegate(t *testing.T) {
	testFactory(t,
		`
		define Lt {
			Left  Expr
			Right Expr
		}

		[Test, Normalize]
		(Lt $left:(Lt $left2:^(Lt $left3:* $right3:^*) $right2:*) $right1:*)
		=>
		(Lt $left $right2)
		`,
		`
		// [Test]
		{
			_lt := _f.mem.lookupNormExpr(left).asLt()
			if _lt != nil {
				left2 := _lt.left()
				_match := false
				_lt2 := _f.mem.lookupNormExpr(_lt.left()).asLt()
				if _lt2 != nil {
					left3 := _lt2.left()
					right3 := _lt2.right()
					if false {
						_match = true
					}
				}

				if !_match {
					right2 := _lt.right()
					right1 := right
					_f.maxSteps--
					_group = _f.ConstructLt(left, right2)
					_f.mem.addAltFingerprint(_ltExpr.fingerprint(), _group)
					return _group
				}
			}
		}
		`)
}

func TestFactoryGenDynamic(t *testing.T) {
	testFactory(t,
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

		[Test, Normalize]
		(InnerJoin $left:(Join|InnerJoin $lowerLeft:* $lowerRight:*) $right:*)
		=>
		((OpName $left) $lowerRight $lowerLeft)
		`,
		`
		// [Test]
		{
			_norm := _f.mem.lookupNormExpr(left)
			if isJoinLookup[_norm.op] || _norm.op == InnerJoinOp {
				_e := makeExpr(_f.mem, left, defaultPhysPropsID)
				lowerLeft := _e.ChildGroup(0)
				lowerRight := _e.ChildGroup(1)
				_f.maxSteps--
				_group = _f.DynamicConstruct(_f.mem.lookupNormExpr(left).op, []GroupID{lowerRight, lowerLeft}, 0)
				_f.mem.addAltFingerprint(_innerJoinExpr.fingerprint(), _group)
				return _group
			}
		}
		`)
}

func testFactory(t *testing.T, in, expected string) {
	r := strings.NewReader(in)
	c := NewCompiler(r)
	compiled, err := c.Compile()
	if err != nil {
		t.Fatal(err)
	}

	var gen FactoryGen
	var buf bytes.Buffer
	gen.Generate(compiled, &buf)

	if testing.Verbose() {
		fmt.Printf("%s\n=>\n\n%s\n", in, buf.String())
	}

	if !strings.Contains(removeWhitespace(buf.String()), removeWhitespace(expected)) {
		t.Fatalf("\nexpected:\n%s\nactual:\n%s", expected, buf.String())
	}
}
