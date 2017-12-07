package v3

import (
	"strings"
	"testing"
)

func TestTreePrinter(t *testing.T) {
	tp := makeTreePrinter()

	tp.Add("root")
	tp.Enter()
	tp.Add("1.1")
	tp.Enter()
	tp.Add("1.1.1")
	tp.Add("1.1.2")
	tp.Enter()
	tp.Add("1.1.2.1")
	tp.Add("1.1.2.2")
	tp.Exit()
	tp.Add("1.1.3")
	tp.Exit()
	tp.Add("1.2")
	tp.Exit()

	res := tp.String()
	exp := `
root
 |- 1.1
 |   |- 1.1.1
 |   |- 1.1.2
 |   |   |- 1.1.2.1
 |   |   |- 1.1.2.2
 |   |- 1.1.3
 |- 1.2
`
	exp = strings.TrimLeft(exp, "\n")
	if res != exp {
		t.Errorf("incorrect result:\n%s", res)
	}
}
