package cat

type Catalog struct {
	// tables maps from name to table metadata.
	tables map[TableName]*Table
}

func NewCatalog() *Catalog {
	return &Catalog{tables: make(map[TableName]*Table)}
}

func (c *Catalog) Table(name TableName) *Table {
	tbl, ok := c.tables[name]
	if !ok {
		fatalf("unable to find table: %s", name)
	}

	return tbl
}

func (c *Catalog) AddTable(tbl *Table) {
	_, ok := c.tables[tbl.Name]
	if ok {
		fatalf("table already exists: %s", tbl.Name)
	}

	c.tables[tbl.Name] = tbl
}
