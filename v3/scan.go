package v3

func init() {
	registerOperator(scanOp, "scan", scanClass{})
}

func newScanExpr(tab *table) *expr {
	return &expr{
		op:      scanOp,
		private: tab,
	}
}

type scanClass struct{}

var _ operatorClass = scanClass{}

func (scanClass) kind() operatorKind {
	return logicalKind | relationalKind
}

func (scanClass) layout() exprLayout {
	return exprLayout{}
}

func (scanClass) format(e *expr, tp *treePrinter) {
	formatRelational(e, tp)
}

func (scanClass) initKeys(e *expr, state *queryState) {
	tab := e.private.(*table)
	props := e.props
	props.foreignKeys = nil

	for _, k := range tab.keys {
		if k.fkey != nil {
			base, ok := state.tables[k.fkey.referenced.name]
			if !ok {
				// The referenced table is not part of the query.
				continue
			}

			var src bitmap
			for _, i := range k.columns {
				src.Add(props.columns[i].index)
			}
			var dest bitmap
			for _, i := range k.fkey.columns {
				dest.Add(base + bitmapIndex(i))
			}

			props.foreignKeys = append(props.foreignKeys, foreignKeyProps{
				src:  src,
				dest: dest,
			})
		}
	}
}

func (scanClass) updateProps(e *expr) {
	e.props.outerCols = e.requiredInputCols().Difference(e.props.outputCols)
}

func (scanClass) requiredProps(required *physicalProps, child int) *physicalProps {
	return nil
}
