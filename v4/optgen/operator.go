package main

//go:generate stringer -type=operator operator.go
type operator int

const (
	unknownOp operator = iota

	rootOp

	defineSetOp
	defineOp
	defineFieldOp

	ruleSetOp
	ruleHeaderOp
	ruleOp
	bindOp
	refOp

	matchTemplateOp
	matchTemplateNamesOp
	matchInvokeOp
	matchFieldsOp
	matchAndOp
	matchNotOp
	matchAnyOp
	matchListOp

	replaceRootOp
	constructOp
	constructListOp

	tagsOp
	stringOp
)
