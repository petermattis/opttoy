package opt

//go:generate optgen -out factory.og.go -pkg opt factory ops/scalar.opt ops/relational.opt ops/enforcer.opt norm/scalar.opt

type Factory struct {
	mem *memo
}

func (f *Factory) Metadata() *Metadata {
	return f.mem.metadata
}

func (f *Factory) StoreList(items []GroupID) ListID {
	return f.mem.storeList(items)
}

func (f *Factory) InternPrivate(private interface{}) PrivateID {
	return f.mem.internPrivate(private)
}

func (f *Factory) isLowerExpr(left, right GroupID) bool {
	return left < right
}
