package optgen

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

var _ = fmt.Println

func TestFactoryGenNegate(t *testing.T) {
	in := `
		define Lt {
			Left  Expr
			Right Expr
		}

		[Test, Normalize]
		(Lt $left:(Lt $left2:^(Lt $left3:* $right3:^*) $right2:*) $right1:*)
		=>
		(Lt $left $right2)
	`

	expected := `
		// [Test]
		{
			_lt := _f.mem.lookupNormExpr(left).asLt()
			if _lt != nil {
				left2 := _lt.left
				_match := false
				_lt2 := _f.mem.lookupNormExpr(_lt.left).asLt()
				if _lt2 != nil {
					left3 := _lt2.left
					right3 := _lt2.right
					if false {
						_match = true
					}
				}

				if !_match {
					right2 := _lt.right
					right1 := right
					_f.maxSteps--
					_group = _f.ConstructLt(left, right2)
					_f.mem.addAltFingerprint(_fingerprint, _group)
					return _group
				}
			}
		}
	`

	test(t, in, expected)
}

func TestFactoryGenDynamic(t *testing.T) {
	in := `
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
	`

	expected := `
		// [Test]
		{
			_norm := _f.mem.lookupNormExpr(left)
			if isJoinLookup[_norm.op] || _norm.op == innerJoinOp {
				_e := makeExpr(_f.mem, left, defaultPhysPropsID)
				lowerLeft := e.Child(0)
				lowerRight := e.Child(1)
				_f.maxSteps--
				_group = _f.DynamicConstruct(_f.mem.lookupNormExpr(left).op, []GroupID{lowerRight, lowerLeft}, 0)
				_f.mem.addAltFingerprint(_fingerprint, _group)
				return _group
			}
		}
	`

	test(t, in, expected)
}

func test(t *testing.T, in, expected string) {
	r := strings.NewReader(in)
	c := NewCompiler(r)
	compiled, err := c.Compile()
	if err != nil {
		t.Fatal(err)
	}

	var gen FactoryGen
	var buf bytes.Buffer
	gen.Generate(compiled, &buf)

	if !strings.Contains(removeWhitespace(buf.String()), removeWhitespace(expected)) {
		t.Fatalf("\nexpected:\n%s\nactual:\n%s", expected, buf.String())
	}
}

func removeWhitespace(s string) string {
	return strings.Replace(strings.Replace(s, " ", "", -1), "\t", "", -1)
}
