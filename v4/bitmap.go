package v4

import (
	"github.com/cockroachdb/cockroach/pkg/util"
)

// Bitmap used for columns.
type bitmap = util.FastIntSet
type bitmapIndex = int
