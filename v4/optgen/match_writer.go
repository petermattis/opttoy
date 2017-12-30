package main

import (
	"fmt"
	"io"
	"strings"
)

type matchWriter struct {
	writer  io.Writer
	nesting int
}

func (w *matchWriter) nest(format string, args ...interface{}) {
	w.writeIndent(format, args...)
	w.nesting++
}

func (w *matchWriter) write(format string, args ...interface{}) {
	fmt.Fprintf(w.writer, format, args...)
}

func (w *matchWriter) writeIndent(format string, args ...interface{}) {
	fmt.Fprintf(w.writer, strings.Repeat("  ", w.nesting))
	fmt.Fprintf(w.writer, format, args...)
}

func (w *matchWriter) unnest(n int, suffix string) {
	for ; n > 0; n-- {
		w.nesting--
		fmt.Fprintf(w.writer, strings.Repeat("  ", w.nesting))
		fmt.Fprintf(w.writer, suffix)
	}
}
