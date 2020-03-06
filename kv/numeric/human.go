package numeric

import (
	"errors"
	"strconv"

	"github.com/zhiqiangxu/util"
)

var (
	errValueIsNil = errors.New("value is nil")
)

// Encode2Human will encode int64 to human readable string
func Encode2Human(v int64) []byte {
	s := strconv.FormatInt(v, 10)
	slice := util.Slice(s)
	return slice
}

// DecodeFromHuman is reverse for Encode2Human
func DecodeFromHuman(b []byte) (n int64, err error) {
	if b == nil {
		err = errValueIsNil
		return
	}
	n, err = strconv.ParseInt(util.String(b), 10, 64)
	return
}
