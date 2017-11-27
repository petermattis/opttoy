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

	matchInvokeOp
	matchFieldsOp
	matchAndOp
	matchNotOp
	matchAnyOp

	replaceListOp
	constructOp

	tagsOp
	stringOp
)
