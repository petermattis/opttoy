package v3

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
)

func testMemo(t *testing.T, sql string) *memo {
	t.Helper()
	stmts, err := parser.Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	p := newPlanner()
	n := len(stmts) - 1
	for _, s := range stmts[:n] {
		p.exec(s)
	}
	e := p.prep(stmts[n])
	m := newMemo()
	m.addRoot(e)
	return m
}

func TestMemoBind(t *testing.T) {
	m := testMemo(t, `
CREATE TABLE a (x INT);
CREATE TABLE b (x INT);
CREATE TABLE c (x INT);
SELECT * FROM a NATURAL JOIN b NATURAL JOIN c;
`)
	if testing.Verbose() {
		fmt.Println(m)
	}

	p := newJoinPattern(innerJoinOp, nil, nil, patternTree)
	e := m.bind(memoLoc{m.root, 0}, p, nil)

	const expected = `[9.0] inner join
  [5.0] inner join
  [6.0] scan
  [8.0] comp (=)
    [2.0] variable
    [7.0] variable
`
	if s := e.MemoString(); expected != s {
		t.Fatalf("expected\n%s\nbut found\n%s", expected, s)
	}

	if m.bind(memoLoc{m.root, 0}, p, e) != nil {
		t.Fatalf("expected a single match, but found\n%s", e.MemoString())
	}
}
