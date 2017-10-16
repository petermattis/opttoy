package v3

type columnIndex uint

type column struct {
	name    string
	notNull bool // TODO(peter): unimplemented
}

type foreignKey struct {
	table string
	key   int
}

type tableKey struct {
	unique  bool
	columns []columnIndex
	fkey    *foreignKey
}

type table struct {
	name    string
	columns []column
	keys    []tableKey // TODO(peter): unimplemented
}
