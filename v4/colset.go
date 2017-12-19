package v4

import (
	"github.com/cockroachdb/cockroach/pkg/util"
)

// Bitmap used for columns.
type colset = util.FastIntSet
type colsetIndex = int
