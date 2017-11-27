package v4

type factory struct {
	memo *memo
}

func newFactory(memo *memo) *factory {
	return &factory{memo: memo}
}

func (f *factory) isLowerExpr(left, right groupID) bool {
	return left < right
}
