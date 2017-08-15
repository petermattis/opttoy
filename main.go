package main

import (
	"bytes"
	"fmt"
	"os"
	"strings"
)

type nodeType int

const (
	join nodeType = iota
	scan
)

type node struct {
	typ         nodeType
	class       string // equivalence class
	classIdx    int
	nodeIdx     int
	left, right *node
}

func parse(s string) *node {
	var n *node
	for _, p := range strings.Split(s, ",") {
		t := &node{typ: scan, class: p}
		if n == nil {
			n = t
		} else {
			n = &node{
				typ:   join,
				left:  n,
				right: t,
			}
		}
	}
	return n
}

func (n *node) Debug() string {
	switch n.typ {
	case join:
		return fmt.Sprintf("(%s ⋈ %s):%d", n.left.Debug(), n.right.Debug(), n.classIdx)
	case scan:
		return fmt.Sprintf("%s:%d", n.class, n.classIdx)
	default:
		return "not reached"
	}
}

func (n *node) String() string {
	switch n.typ {
	case join:
		return fmt.Sprintf("(%s ⋈ %s)", n.left, n.right)
	case scan:
		return fmt.Sprintf("%s", n.class)
	default:
		return "not reached"
	}
}

type class struct {
	m     map[string]int
	nodes []*node
}

func newClass() *class {
	return &class{
		m: make(map[string]int),
	}
}

func (c *class) add(n *node) bool {
	id := n.String()
	i, ok := c.m[id]
	if ok {
		return false
	}
	i = len(c.nodes)
	c.nodes = append(c.nodes, n)
	c.m[id] = i
	n.nodeIdx = i
	return true
}

type memo struct {
	nodeMap  map[string]int
	classMap map[string]int
	classes  []*class
}

func newMemo() *memo {
	return &memo{
		nodeMap:  make(map[string]int),
		classMap: make(map[string]int),
	}
}

func (m *memo) build(n *node) {
	switch n.typ {
	case join:
		m.add(n)
		m.build(n.left)
		m.build(n.right)

	case scan:
		m.add(n)
	}
}

// A ⋈ B => B ⋈ A
func (m *memo) commute(n *node) *node {
	if n.typ != join {
		return nil
	}
	return &node{
		typ:   join,
		class: n.class,
		left:  n.right,
		right: n.left,
	}
}

// (A ⋈ B) ⋈ C  => A ⋈ (B ⋈ C)
func (m *memo) associate(n *node) *node {
	if n.typ != join || n.left.typ != join {
		return nil
	}
	return &node{
		typ:   join,
		class: n.class,
		left:  n.left.left,
		right: &node{
			typ:   join,
			left:  n.left.right,
			right: n.right,
		},
	}
}

func (m *memo) expand() int {
	var count int
	for _, c := range m.classes {
		for _, n := range c.nodes {
			if t := m.commute(n); t != nil && m.add(t) {
				count++
			}
			if t := m.associate(n); t != nil && m.add(t) {
				count++
				if m.add(t.right) {
					count++
				}
			}
		}
	}
	return count
}

func (m *memo) expandAll() {
	fmt.Println(m)
	for {
		n := m.expand()
		if n == 0 {
			break
		}
		fmt.Printf("%d expansions\n%s\n", n, m)
	}
}

func (m *memo) add(n *node) bool {
	id := n.String()
	if n.class == "" {
		n.class = id
	}
	if _, ok := m.nodeMap[id]; ok {
		return false
	}
	i, ok := m.classMap[n.class]
	if !ok {
		i = len(m.classes)
		c := newClass()
		m.classes = append(m.classes, c)
		m.classMap[n.class] = i
	}
	m.nodeMap[id] = i
	n.classIdx = i
	return m.classes[i].add(n)
}

func (m *memo) list(n *node) {
	for _, n := range m.classes[n.classIdx].nodes {
		fmt.Println(n)
	}
}

func (m *memo) String() string {
	var buf bytes.Buffer
	for i, c := range m.classes {
		fmt.Fprintf(&buf, "%d:", i)
		for _, n := range c.nodes {
			fmt.Fprintf(&buf, " [%s]", n.Debug())
		}
		fmt.Fprintf(&buf, "\n")
	}
	return buf.String()
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "usage: opttoy <query>\n")
		os.Exit(1)
	}
	n := parse(os.Args[1])
	m := newMemo()
	m.build(n)
	m.expandAll()
	m.list(n)
}
