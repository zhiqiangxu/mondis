package ddl

import (
	"time"

	"github.com/zhiqiangxu/mondis/document/schema"
)

// Options for ddl
type Options struct {
	MetaCacheHandle *schema.Handle
	Lease           time.Duration
}
