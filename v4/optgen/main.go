package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
)

var (
	errInvalidArgCount     = errors.New("invalid number of arguments")
	errUnrecognizedCommand = errors.New("unrecognized command")
)

var (
	pkg = flag.String("pkg", "opt", "package name used in generated files")
	out = flag.String("out", "", "output file name of generated code")
)

func main() {
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) < 2 {
		flag.Usage()
		exit(errInvalidArgCount)
	}

	cmd := args[0]
	switch cmd {
	case "compile":
	case "exprs":
	case "factory":
	case "ops":
	case "optimizer":
	case "visitor":

	default:
		flag.Usage()
		exit(errUnrecognizedCommand)
	}

	sources := flag.Args()[1:]
	readers := make([]io.Reader, len(sources))
	for i, name := range sources {
		file, err := os.Open(name)
		if err != nil {
			exit(err)
		}

		defer file.Close()
		readers[i] = file
	}

	compiler := NewCompiler(io.MultiReader(readers...))
	compiled, err := compiler.Compile()
	if err != nil {
		exit(err)
	}

	var writer io.Writer
	if *out != "" {
		file, err := os.Create(*out)
		if err != nil {
			exit(err)
		}

		defer file.Close()
		writer = file
	} else {
		writer = os.Stderr
	}

	gen := NewGenerator(*pkg, compiled)
	switch cmd {
	case "compile":
		writer.Write([]byte(compiled.String()))

	case "exprs":
		err = gen.GenerateExprs(writer)

	case "factory":
		err = gen.GenerateFactory(writer)

	case "ops":
		err = gen.GenerateOps(writer)

	case "optimizer":
		err = gen.GenerateOptimizer(writer)
	}

	if err != nil {
		exit(err)
	}
}

// usage is a replacement usage function for the flags package.
func usage() {
	fmt.Fprintf(os.Stderr, "Optgen is a tool for generating cost-based optimizers.\n\n")

	fmt.Fprintf(os.Stderr, "It compiles source files that use a custom syntax to define expressions,\n")
	fmt.Fprintf(os.Stderr, "match expression patterns, and generate replacement expressions.\n\n")

	fmt.Fprintf(os.Stderr, "Usage:\n")

	fmt.Fprintf(os.Stderr, "\toptgen command [flags] sources...\n\n")

	fmt.Fprintf(os.Stderr, "The commands are:\n\n")
	fmt.Fprintf(os.Stderr, "\tcompile    generates the optgen compiled format\n")
	fmt.Fprintf(os.Stderr, "\texprs      generates expression definitions and functions\n")
	fmt.Fprintf(os.Stderr, "\tfactory    generates expression tree creation and normalization functions\n")
	fmt.Fprintf(os.Stderr, "\tops        generates operator definitions and functions\n")
	fmt.Fprintf(os.Stderr, "\toptimizer  generates exploration and implementation functions\n")
	fmt.Fprintf(os.Stderr, "\n")

	fmt.Fprintf(os.Stderr, "Flags:\n")

	flag.PrintDefaults()

	fmt.Fprintf(os.Stderr, "\n")
}

func exit(err error) {
	fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
	os.Exit(2)
}