package exec

import (
	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/petermattis/opttoy/v4/cat"
)

type createTable struct {
	catalog *cat.Catalog
	tbl     *cat.Table
}

func (ct *createTable) execute(stmt *tree.CreateTable) *cat.Table {
	tn, err := stmt.Table.Normalize()
	if err != nil {
		fatalf("%s", err)
	}

	ct.tbl = &cat.Table{Name: cat.TableName(tn.Table())}

	for _, def := range stmt.Defs {
		switch def := def.(type) {
		case *tree.ColumnTableDef:
			ct.addColumn(def)

		case *tree.UniqueConstraintTableDef:
			ct.addUniqueConstraintKey(def)

		case *tree.IndexTableDef:
			ct.addIndexKey(def)

		case *tree.ForeignKeyConstraintTableDef:
			ct.addTableForeignKey(def)

		default:
			unimplemented("%T", def)
		}
	}

	// Add the new table to the catalog.
	ct.catalog.AddTable(ct.tbl)

	return ct.tbl
}

func (ct *createTable) addColumn(def *tree.ColumnTableDef) {
	notNull := def.PrimaryKey || (def.Nullable.Nullability == tree.NotNull)
	typ := coltypes.CastTargetToDatumType(def.Type)
	col := cat.Column{Name: cat.ColumnName(def.Name), NotNull: notNull, Type: typ}

	ord := ct.tbl.AddColumn(&col)

	if def.Unique || def.PrimaryKey {
		key := ct.addKey(&cat.TableKey{
			Primary: def.PrimaryKey,
			Unique:  true,
			Columns: []cat.ColumnOrdinal{ord},
		})

		if key.Name == "" {
			if def.PrimaryKey {
				key.Name = "primary"
			} else {
				key.Name = string(def.Name) + "_idx"
			}
		}
	}

	if def.HasFKConstraint() {
		refTable, err := def.References.Table.Normalize()
		if err != nil {
			fatalf("%s", err)
		}

		ref := ct.catalog.Table(cat.TableName(refTable.Table()))

		var refCols []cat.ColumnOrdinal
		if def.References.Col != "" {
			refCols = []cat.ColumnOrdinal{ct.tbl.ColumnOrdinal(cat.ColumnName(def.References.Col))}
		} else {
			for _, key := range ref.Keys {
				if key.Primary {
					refCols = key.Columns
					break
				}
			}

			if refCols == nil {
				fatalf("%s does not contain a primary key", ref.Name)
			}
		}

		ct.addForeignKey(ref, []cat.ColumnOrdinal{ord}, refCols)
	}
}

func (ct *createTable) addUniqueConstraintKey(def *tree.UniqueConstraintTableDef) {
	cols := ct.extractColumns(&def.IndexTableDef)
	if def.PrimaryKey {
		for _, i := range cols {
			ct.tbl.Columns[i].NotNull = true
		}
	}

	key := ct.addKey(&cat.TableKey{
		Primary: def.PrimaryKey,
		Unique:  true,
		Columns: cols,
	})

	if key.Name == "" {
		key.Name = string(def.Name)
		if key.Name == "" {
			key.Name = "primary"
		}
	}
}

func (ct *createTable) addIndexKey(def *tree.IndexTableDef) {
	key := ct.addKey(&cat.TableKey{
		Unique:  true,
		Columns: ct.extractColumns(def),
	})

	if key.Name == "" {
		key.Name = string(def.Name)
	}
}

func (ct *createTable) addTableForeignKey(def *tree.ForeignKeyConstraintTableDef) {
	refTable, err := def.Table.Normalize()
	if err != nil {
		fatalf("%s", err)
	}

	ref := ct.catalog.Table(cat.TableName(refTable.Table()))

	var toCols []cat.ColumnOrdinal
	if len(def.ToCols) == 0 {
		for _, key := range ref.Keys {
			if key.Primary {
				toCols = key.Columns
				break
			}
		}

		if toCols == nil {
			fatalf("%s does not contain a primary key", ref.Name)
		}
	} else {
		toCols = ct.extractNames(def.ToCols)
	}

	if len(def.FromCols) != len(toCols) {
		fatalf("invalid foreign key specification: %s(%s) -> %s(%s)",
			ct.tbl.Name, def.FromCols, ref.Name, def.ToCols)
	}

	ct.addForeignKey(ref, ct.extractNames(def.FromCols), toCols)
}

func (ct *createTable) addKey(key *cat.TableKey) *cat.TableKey {
	existing := ct.getKey(key)
	if existing != nil {
		existing.Primary = existing.Primary || key.Primary
		existing.Unique = existing.Unique || key.Unique
		existing.NotNull = existing.NotNull || key.NotNull
		return existing
	}

	key.NotNull = true
	for _, i := range key.Columns {
		key.NotNull = key.NotNull && ct.tbl.Columns[i].NotNull
	}

	ct.tbl.AddKey(key)
	return key
}

func (ct *createTable) getKey(key *cat.TableKey) *cat.TableKey {
	for i := range ct.tbl.Keys {
		existing := &ct.tbl.Keys[i]
		if existing.EqualColumns(key) {
			return existing
		}
	}

	return nil
}

func (ct *createTable) addForeignKey(dest *cat.Table, srcColumns, destColumns []cat.ColumnOrdinal) {
	srcKey := ct.addKey(&cat.TableKey{Columns: srcColumns})

	if srcKey.Fkey != nil {
		fatalf("foreign key already defined for %d", srcColumns)
	}

	srcKey.Fkey = &cat.ForeignKey{
		Referenced: dest,
		Columns:    destColumns,
	}
}

func (ct *createTable) extractColumns(def *tree.IndexTableDef) []cat.ColumnOrdinal {
	res := make([]cat.ColumnOrdinal, len(def.Columns))
	for i, col := range def.Columns {
		res[i] = ct.tbl.ColumnOrdinal(cat.ColumnName(col.Column))
	}

	return res
}

func (ct *createTable) extractNames(names tree.NameList) []cat.ColumnOrdinal {
	res := make([]cat.ColumnOrdinal, len(names))
	for i, name := range names {
		res[i] = ct.tbl.ColumnOrdinal(cat.ColumnName(name))
	}

	return res
}
