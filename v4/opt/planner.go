package opt

import (
	"github.com/petermattis/opttoy/v4/cat"
)

type Planner struct {
	mem     *memo
	factory *Factory
}

func NewPlanner(catalog *cat.Catalog) *Planner {
	mem := newMemo(catalog)
	factory := &Factory{mem: mem}
	return &Planner{mem: mem, factory: factory}
}

func (p *Planner) Metadata() *Metadata {
	return p.mem.metadata
}

func (p *Planner) Factory() *Factory {
	return p.factory
}

func (p *Planner) Optimize(root GroupID, required *PhysicalProps) Expr {
	o := newOptimizer(p.factory, 0)
	requiredID := p.mem.internPhysicalProps(required)
	return o.optimize(root, requiredID)
}
