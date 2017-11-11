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
	fmt.Println(e)
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
	fmt.Println(m)

	p := newJoinPattern(innerJoinOp, nil, nil, patternTree)
	e := m.bind(memoLoc{m.root, 0}, p, nil)
	fmt.Println(e.MemoString())
}
