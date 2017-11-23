package main

//go:generate stringer -type=operator operator.go
type operator int

const (
	unknownOp operator = iota

	rootOp

	defineListOp
	defineOp
	defineFieldOp

	ruleListOp
	ruleHeaderOp
	ruleOp
	bindOp
	refOp

	matchListOp
	matchFieldsOp
	matchStringOp
	matchAnyOp

	replaceListOp
	constructOp

	stringOp
)
