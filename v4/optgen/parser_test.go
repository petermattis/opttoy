package optgen

import (
	"fmt"
	"strings"
	"testing"
)

var _ = fmt.Println

func TestParserDefine(t *testing.T) {
	testParser(t,
		`
		define Lt {
			Left  Expr
			Right Expr
		}
		`,
		`
		(Root
			Defines=(DefineSet
				(Define
					Name="Lt"
					Tags=(Tags)
					(DefineField Name="Left" Type="Expr")
					(DefineField Name="Right" Type="Expr")
				)
			)
			Rules=(RuleSet)
		)
		`)
}

func TestParserPattern(t *testing.T) {
	testParser(t,
		`
		[EliminateVariable]
		(Eq $left:^(Variable) $right:(Variable)) => (Eq $right $left)
		`,
		`
		(Root
			Defines=(DefineSet)
			Rules=(RuleSet
				(Rule
					Header=(RuleHeader Name="EliminateVariable" Tags=(Tags))
					Match=(MatchFields
						Names=(MatchNames "Eq")
						(Bind
							Label="left"
							Target=(MatchNot
								(MatchFields
									Names=(MatchNames "Variable")
								)
							)
						)
						(Bind
							Label="right"
							Target=(MatchFields
								Names=(MatchNames "Variable")
							)
						)
					)
					Replace=(Construct
						OpName="Eq"
						(Ref Label="right")
						(Ref Label="left")
					)
				)
			)
		)
		`)
}

func TestParserMatchList(t *testing.T) {
	testParser(t,
		`
		[ExtractSubquery]
		(Select * $filter:[ ... (Exists $subquery:*) & (IsMatch $subquery) ...]) => $subquery
		`,
		`
		(Rule
			Header=(RuleHeader Name="ExtractSubquery" Tags=(Tags))
			Match=(MatchFields
				Names=(MatchNames "Select")
				(MatchAny)
				(Bind
					Label="filter"
					Target=(MatchList
						(MatchAnd
							(MatchFields
								Names=(MatchNames "Exists")
								(Bind Label="subquery" Target=(MatchAny))
							)
							(MatchInvoke
								FuncName="IsMatch"
								(Ref Label="subquery")
							)
						)
					)
				)
			)
			Replace=(Ref Label="subquery")
		)
		`)
}

func testParser(t *testing.T, in, expected string) {
	r := strings.NewReader(in)
	p := NewParser(r)
	root, err := p.Parse()
	if err != nil {
		t.Fatal(err)
	}

	if testing.Verbose() {
		fmt.Printf("%s\n=>\n\n%s\n", in, root.String())
	}

	if !strings.Contains(removeWhitespace(root.String()), removeWhitespace(expected)) {
		t.Fatalf("\nexpected:\n%s\nactual:\n%s", expected, root.String())
	}
}

func removeWhitespace(s string) string {
	return strings.Trim(strings.Replace(strings.Replace(s, " ", "", -1), "\t", "", -1), " \t\r\n")
}
