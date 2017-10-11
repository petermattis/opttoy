package v2

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
)

var (
	logicTestData    = flag.String("d", "testdata/[^.]*", "test data glob")
	rewriteTestFiles = flag.Bool("rewrite-testfiles", false, "")
)

type lineScanner struct {
	*bufio.Scanner
	line int
}

func newLineScanner(r io.Reader) *lineScanner {
	return &lineScanner{
		Scanner: bufio.NewScanner(r),
		line:    0,
	}
}

func (l *lineScanner) Scan() bool {
	ok := l.Scanner.Scan()
	if ok {
		l.line++
	}
	return ok
}

type testdata struct {
	pos      string // file and line number
	cmd      string // exec, query, ...
	sql      string
	stmt     parser.Statement
	expected string
}

type testdataReader struct {
	path    string
	file    *os.File
	scanner *lineScanner
	data    testdata
	rewrite *bytes.Buffer
}

func newTestdataReader(t *testing.T, path string) *testdataReader {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	var rewrite *bytes.Buffer
	if *rewriteTestFiles {
		rewrite = &bytes.Buffer{}
	}
	return &testdataReader{
		path:    path,
		file:    file,
		scanner: newLineScanner(file),
		rewrite: rewrite,
	}
}

func (r *testdataReader) Close() error {
	return r.file.Close()
}

func (r *testdataReader) Next(t *testing.T) bool {
	t.Helper()

	r.data = testdata{}
	for r.scanner.Scan() {
		line := r.scanner.Text()
		r.emit(line)

		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		cmd := fields[0]
		if strings.HasPrefix(cmd, "#") {
			// Skip comment lines.
			continue
		}
		r.data.pos = fmt.Sprintf("%s:%d", r.path, r.scanner.line)
		r.data.cmd = cmd

		var buf bytes.Buffer
		var separator bool
		for r.scanner.Scan() {
			line := r.scanner.Text()
			if strings.TrimSpace(line) == "" {
				break
			}

			r.emit(line)
			if line == "----" {
				separator = true
				break
			}
			fmt.Fprintln(&buf, line)
		}

		r.data.sql = strings.TrimSpace(buf.String())
		stmt, err := parser.ParseOne(r.data.sql)
		if err != nil {
			t.Fatal(err)
		}
		r.data.stmt = stmt

		if separator {
			buf.Reset()
			for r.scanner.Scan() {
				line := r.scanner.Text()
				if strings.TrimSpace(line) == "" {
					break
				}
				fmt.Fprintln(&buf, line)
			}
			r.data.expected = buf.String()
		}
		return true
	}
	return false
}

func (r *testdataReader) emit(s string) {
	if r.rewrite != nil {
		r.rewrite.WriteString(s)
		r.rewrite.WriteString("\n")
	}
}

func runTest(t *testing.T, path string, f func(d *testdata) string) {
	t.Helper()

	r := newTestdataReader(t, path)
	for r.Next(t) {
		d := &r.data
		str := f(d)
		if r.rewrite != nil {
			r.emit(str)
		} else if d.expected != str {
			t.Fatalf("%s: %s\nexpected:\n%s\nfound:\n%s", d.pos, d.sql, d.expected, str)
		} else if testing.Verbose() {
			fmt.Printf("%s:\n%s\n----\n%s", d.pos, d.sql, str)
		}
	}

	if r.rewrite != nil {
		data := r.rewrite.Bytes()
		if l := len(data); l > 2 && data[l-1] == '\n' && data[l-2] == '\n' {
			data = data[:l-1]
		}
		err := ioutil.WriteFile(path, data, 0644)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestLogic(t *testing.T) {
	paths, err := filepath.Glob(*logicTestData)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatalf("no testfiles found matching: %s", *logicTestData)
	}

	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			e := newExecutor()
			runTest(t, path, func(d *testdata) string {
				switch d.cmd {
				case "exec":
					e.exec(d.sql)
					return ""
				}

				expr, state := e.prep(d.stmt)
				for _, cmd := range strings.Split(d.cmd, ",") {
					switch cmd {
					case "prep":
						// Already done.
					case "push_down":
						expr.pushDownFilters(state)
					case "decorrelate":
						expr.decorrelate(state)
					default:
						t.Fatalf("unknown command: %s", cmd)
					}
				}
				return expr.String()
			})
		})
	}
}
