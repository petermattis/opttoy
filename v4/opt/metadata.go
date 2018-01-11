package opt

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/petermattis/opttoy/v4/cat"
)

// ColSet efficiently stores an unordered set of column indexes.
type ColSet = util.FastIntSet

type ColSets []ColSet

type ColumnIndex int32

type TableIndex int32

type TableMetadata struct {
	Table    *cat.Table
	Ordering Ordering
}

type Metadata struct {
	catalog *cat.Catalog

	cols []string

	// nextCol keeps track of the next index for a column.
	nextCol ColumnIndex

	// tables maps from memo table index to the catalog metadata for the table.
	// The table index is the index of the first column in the table. The
	// remaining columns form a contiguous group following that index.
	tables map[TableIndex]*TableMetadata
}

func newMetadata(catalog *cat.Catalog) *Metadata {
	return &Metadata{catalog: catalog, cols: make([]string, 1), tables: make(map[TableIndex]*TableMetadata)}
}

func (md *Metadata) Catalog() *cat.Catalog {
	return md.catalog
}

func (md *Metadata) AddColumn(label string) ColumnIndex {
	// Skip index 0 so that it is reserved for "unknown column".
	md.nextCol++
	md.cols = append(md.cols, label)
	return md.nextCol
}

func (md *Metadata) ColumnLabel(index ColumnIndex) string {
	if index == 0 {
		panic("uninitialized column id 0")
	}

	return md.cols[index]
}

// Every reference to a table in the query gets a new set of output column
// indexes. Consider the query:
//
//   SELECT * FROM a AS l JOIN a AS r ON (l.x = r.y)
//
// In this query, `l.x` is not equivalent to `r.x` and `l.y` is not
// equivalent to `r.y`. In order to achieve this, we need to give these
// columns different indexes.
func (md *Metadata) AddTable(tbl *cat.Table) TableIndex {
	tblIndex := TableIndex(md.nextCol + 1)

	for i := range tbl.Columns {
		col := &tbl.Columns[i]
		if tbl.Name == "" {
			md.AddColumn(string(col.Name))
		} else {
			md.AddColumn(fmt.Sprintf("%s.%s", tbl.Name, col.Name))
		}
	}

	// Tables are naturally ordered by primary key.
	primary := tbl.PrimaryKey()

	ordering := make(Ordering, len(primary.Columns))
	for i, ord := range primary.Columns {
		ordering[i] = md.TableColumn(tblIndex, ord)
	}

	md.tables[tblIndex] = &TableMetadata{Table: tbl, Ordering: ordering}

	return tblIndex
}

func (md *Metadata) Table(index TableIndex) *TableMetadata {
	return md.tables[index]
}

func (md *Metadata) TableColumn(index TableIndex, ord cat.ColumnOrdinal) ColumnIndex {
	return ColumnIndex(int(index) + int(ord))
}

func (cs ColSets) Copy() ColSets {
	new := make(ColSets, len(cs))
	for i := 0; i < len(cs); i++ {
		new[i] = cs[i].Copy()
	}

	return new
}
