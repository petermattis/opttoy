pushd ./optgen/ && go build && popd && ./optgen/optgen -out operator.og.go -pkg v4 ops scalar_ops.opt scalar_norm.opt relational_ops.opt enforcer_ops.opt
pushd ./optgen/ && go build && popd && ./optgen/optgen -out expr.og.go -pkg v4 exprs scalar_ops.opt scalar_norm.opt relational_ops.opt enforcer_ops.opt
pushd ./optgen/ && go build && popd && ./optgen/optgen -out factory.og.go -pkg v4 factory scalar_ops.opt scalar_norm.opt relational_ops.opt enforcer_ops.opt
