package optgen

//go:generate stringer -type=Operator operator.go
type Operator int

const (
	UnknownOp Operator = iota

	RootOp

	DefineSetOp
	DefineOp
	DefineFieldOp

	RuleSetOp
	RuleHeaderOp
	RuleOp
	BindOp
	RefOp

	MatchNamesOp
	MatchInvokeOp
	MatchFieldsOp
	MatchAndOp
	MatchNotOp
	MatchAnyOp
	MatchListOp

	ReplaceRootOp
	ConstructOp
	ConstructListOp

	TagsOp
	StringOp
	OpNameOp
)
