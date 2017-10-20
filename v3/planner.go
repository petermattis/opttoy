// TODO(peter):
//
// logical properties
// - required columns
// - derived columns
// - keys, candidate keys and foreign keys
// - column value constraints
// - required ordering
//
// physical properties
// - provided ordering
//
// scalar properties
// - monotonic (ordering)
// - not null
// - constness
// - type
//
// cost-agnostic transformations
// - predicate push down
// - join elimination
// - unnesting
//
// cost-based transformations
// - join re-ordering
// - group-by pull-up
// - group-by push-down

package v3

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
)

func unimplemented(format string, args ...interface{}) {
	panic("unimplemented: " + fmt.Sprintf(format, args...))
}

func fatalf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}

type planner struct {
	catalog map[string]*table
}

func newPlanner() *planner {
	return &planner{
		catalog: make(map[string]*table),
	}
}

func (p *planner) exec(stmt parser.Statement) string {
	switch stmt := stmt.(type) {
	case *parser.CreateTable:
		tab := createTable(p.catalog, stmt)
		return tab.String()
	default:
		unimplemented("%T", stmt)
	}
	return ""
}

func (p *planner) prep(stmt parser.Statement) *expr {
	return build(stmt, &logicalProps{
		state: &queryState{
			catalog: p.catalog,
			tables:  make(map[string]bitmapIndex),
		},
	})
}
