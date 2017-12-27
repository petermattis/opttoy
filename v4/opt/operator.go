package opt

import "fmt"

//go:generate optgen -out operator.og.go -pkg opt ops ops/scalar.opt ops/relational.opt ops/enforcer.opt
type Operator uint16

func (i Operator) String() string {
	if i >= Operator(len(opNames)-1) {
		return fmt.Sprintf("Operator(%d)", i)
	}

	return opNames[opIndexes[i]:opIndexes[i+1]]
}
